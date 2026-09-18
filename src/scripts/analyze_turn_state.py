"""Analyze X-Codex-Turn-State samples: decode base64, inspect the Fernet
layout and extract the embedded metadata (e.g. the timestamp).

Both samples are synthetic tokens with the same byte layout as the production
values; no captured data is contained in this file.
"""

import base64
import struct
from datetime import datetime, timezone

def synthetic_token(timestamp, seed):
    # Deliberately patterned bytes: valid layout, no real IV, ciphertext or MAC.
    raw = b"\x80" + struct.pack(">Q", timestamp)
    raw += bytes((index + seed) % 256 for index in range(208))
    return base64.urlsafe_b64encode(raw).decode("ascii")


VALUES = {
    "sample A (synthetic)": synthetic_token(1719800000, 0),
    "sample B (synthetic)": synthetic_token(1719800193, 1),
}

for name, value in VALUES.items():
    raw = base64.urlsafe_b64decode(value)
    version = raw[0]
    timestamp = struct.unpack(">Q", raw[1:9])[0]
    generated = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    iv = raw[9:25]
    mac = raw[-32:]
    cipher = raw[25:-32]
    printable = sum(1 for byte in cipher if 32 <= byte < 127)
    print(f"== {name}")
    print(f"   encoded length: {len(value)} chars | decoded length: {len(raw)} bytes")
    print(f"   version byte: 0x{version:02x}")
    print(f"   timestamp: {timestamp} -> {generated.isoformat()}")
    print(f"   iv (16B): {iv.hex()}")
    print(f"   ciphertext: {len(cipher)} bytes | printable ratio: {printable/len(cipher):.3f}")
    print(f"   hmac: {len(mac)} bytes")
    print(f"   ciphertext head: {cipher[:32].hex()}")
    print()
