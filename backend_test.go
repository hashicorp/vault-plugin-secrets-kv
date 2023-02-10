package kv

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/hashicorp/vault/sdk/logical"
)

type expectedEvent struct {
	eventType string
	path      string
}

type mockEventsSender struct {
	eventsProcessed []*logical.EventReceived
}

func (m *mockEventsSender) Send(ctx context.Context, eventType logical.EventType, event *logical.EventData) error {
	if m == nil {
		return nil
	}
	m.eventsProcessed = append(m.eventsProcessed, &logical.EventReceived{
		EventType: string(eventType),
		Event:     event,
	})

	return nil
}

func (m *mockEventsSender) expectEvents(t *testing.T, expected []expectedEvent) {
	t.Helper()
	if len(m.eventsProcessed) != len(expected) {
		t.Fatalf("Expected events: %v\nEvents processed: %v", expected, m.eventsProcessed)
	}
	for i, e := range expected {
		if e.eventType != m.eventsProcessed[i].EventType {
			t.Fatalf("Mismatched event type at index %d. Expected %s, got %s\n%v", i, e.eventType, m.eventsProcessed[i].EventType, m.eventsProcessed)
		}
		metadata := make(map[string]interface{})
		if err := json.Unmarshal(m.eventsProcessed[i].Event.Metadata, &metadata); err != nil {
			t.Fatal(err)
		}
		if e.path != metadata["path"].(string) {
			t.Fatalf("Mismatched path at index %d. Expected %s, got %s\n%v", i, e.path, metadata["path"].(string), m.eventsProcessed)
		}
	}
}
