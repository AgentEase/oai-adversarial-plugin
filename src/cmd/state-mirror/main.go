// state-mirror serves a read-only public page and a separate signed ingestion
// listener. It never connects to CPA and contains no CPA management credential.
package main

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"cpa-timezone/internal/statemirror"
)

//go:embed web/index.html web/app.js web/style.css web/favicon.png
var assets embed.FS

func publicHandler(receiver *statemirror.Receiver) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "read only", 405)
			return
		}
		switch r.URL.Path {
		case "/state/snapshot":
			receiver.JSON(w, r)
		case "/state/events":
			receiver.Events(w, r)
		case "/state", "/state/":
			serveAsset(w, "web/index.html", "text/html; charset=utf-8")
		case "/state/app.js":
			serveAsset(w, "web/app.js", "text/javascript; charset=utf-8")
		case "/state/style.css":
			serveAsset(w, "web/style.css", "text/css; charset=utf-8")
		case "/state/favicon.png":
			serveAsset(w, "web/favicon.png", "image/png")
		default:
			http.NotFound(w, r)
		}
	})
}

func serveAsset(w http.ResponseWriter, path, contentType string) {
	b, err := assets.ReadFile(path)
	if err != nil {
		http.Error(w, "asset unavailable", 500)
		return
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write(b)
}

func main() {
	public := flag.String("public", "127.0.0.1:8768", "Public read-only listener (place behind HTTPS reverse proxy)")
	ingest := flag.String("ingest", "127.0.0.1:8769", "Private signed ingestion listener; never reverse proxy publicly")
	file := flag.String("data", "mirror-data/state.json", "Latest snapshot file")
	keyFile := flag.String("key-file", os.Getenv("LKS_MIRROR_KEY_FILE"), "Dedicated shared signing key file (minimum 32 bytes)")
	flag.Parse()
	if err := run(*public, *ingest, *file, *keyFile); err != nil {
		fmt.Fprint(os.Stderr, "[ERROR] - State mirror stopped: "+err.Error()+"\n")
		os.Exit(1)
	}
}

func run(public, ingest, file, keyFile string) error {
	key, err := statemirror.ReadKey(keyFile)
	if err != nil {
		return err
	}
	receiver, err := statemirror.NewReceiver(file, key)
	if err != nil {
		return err
	}
	a, err := net.Listen("tcp", public)
	if err != nil {
		return fmt.Errorf("public listener unavailable")
	}
	defer a.Close()
	b, err := net.Listen("tcp", ingest)
	if err != nil {
		return fmt.Errorf("ingestion listener unavailable")
	}
	defer b.Close()
	readServer := &http.Server{Handler: publicHandler(receiver), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 8192}
	writeServer := &http.Server{Handler: http.HandlerFunc(receiver.Ingest), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	errors := make(chan error, 2)
	go func() { errors <- readServer.Serve(a) }()
	go func() { errors <- writeServer.Serve(b) }()
	fmt.Fprint(os.Stderr, "[INFO] - State mirror listeners ready\n")
	var result error
	select {
	case <-ctx.Done():
	case <-errors:
		result = fmt.Errorf("mirror listener stopped unexpectedly")
	}
	// Close also terminates long-lived SSE streams on shutdown.
	_ = readServer.Close()
	_ = writeServer.Close()
	return result
}
