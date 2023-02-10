package kv

import (
	"context"
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

func (m *mockEventsSender) expectEvents(t *testing.T, expectedEvents []expectedEvent) {
	t.Helper()
	if len(m.eventsProcessed) != len(expectedEvents) {
		t.Fatalf("Expected events: %v\nEvents processed: %v", expectedEvents, m.eventsProcessed)
	}
	for i, expected := range expectedEvents {
		actual := m.eventsProcessed[i]
		if expected.eventType != actual.EventType {
			t.Fatalf("Mismatched event type at index %d. Expected %s, got %s\n%v", i, expected.eventType, actual.EventType, m.eventsProcessed)
		}
		actualPath := actual.Event.Metadata.Fields["path"].GetStringValue()
		if expected.path != actualPath {
			t.Fatalf("Mismatched path at index %d. Expected %s, got %s\n%v", i, expected.path, actualPath, m.eventsProcessed)
		}
	}
}
