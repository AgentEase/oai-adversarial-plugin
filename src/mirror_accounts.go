package main

import (
	"cpa-timezone/internal/statemirror"
	"time"
)

// The public page receives coverage counts, never auth IDs or account labels.
// Multiple accounts have different expiry times; do not invent one shared TTL.
func aggregateAccountMirror(rows []statemirror.Model, models []string, now time.Time) []statemirror.Model {
	groups := map[string][]statemirror.Model{}
	for _, row := range rows {
		model := targetModel(row.Name)
		row.Name = model
		groups[model] = append(groups[model], row)
	}
	result := make([]statemirror.Model, 0, len(models))
	for _, model := range models {
		items := groups[model]
		row := statemirror.Model{Name: model, Detection: degradationDetectionEnabled(model, ""), Activity: "unavailable", Evidence: "none"}
		if !row.Detection {
			row.Activity = "unchecked"
		}
		if len(items) > 0 {
			row = items[0]
		}
		if row.Detection {
			row.FailureAttempts = 0
			activityPriority := map[string]int{"unavailable": 0, "paused": 1, "halted": 2, "manual": 3, "standby": 4, "sleeping": 5, "queued": 6, "probing": 7, "stopping": 8}
			row.AccountsTotal = len(items)
			for _, item := range items {
				if ticket := item.Active; ticket != nil && ticket.Valid {
					until, err := time.Parse(time.RFC3339Nano, ticket.ExpiresAt)
					if err == nil && now.Before(until) {
						row.AccountsReady++
					}
				}
				if activityPriority[item.Activity] > activityPriority[row.Activity] {
					row.Activity = item.Activity
				}
				if item.Evidence != "none" {
					row.Evidence = item.Evidence
					row.FailureAttempts += item.FailureAttempts
				}
			}
			if len(items) > 1 {
				row.Active = nil
				row.Candidate = nil
			}
		}
		result = append(result, row)
	}
	return result
}
