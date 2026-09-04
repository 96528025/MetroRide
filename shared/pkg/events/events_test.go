package events

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestNewEnvelopePreservesMetadataAndPayload(t *testing.T) {
	payload := RideRequested{
		RideID:      "ride-123",
		RiderID:     "rider-456",
		PickupLat:   37.775,
		PickupLng:   -122.419,
		DropoffLat:  37.789,
		DropoffLng:  -122.401,
		RequestedAt: "2026-09-04T12:00:00Z",
	}
	before := time.Now().UTC()

	envelope, err := NewEnvelope("event-1", TypeRideRequested, "rider-service", "ride-123", payload)
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	after := time.Now().UTC()

	if envelope.ID != "event-1" {
		t.Errorf("ID = %q, want %q", envelope.ID, "event-1")
	}
	if envelope.Type != TypeRideRequested {
		t.Errorf("Type = %q, want %q", envelope.Type, TypeRideRequested)
	}
	if envelope.Source != "rider-service" {
		t.Errorf("Source = %q, want %q", envelope.Source, "rider-service")
	}
	if envelope.CorrelationID != "ride-123" {
		t.Errorf("CorrelationID = %q, want %q", envelope.CorrelationID, "ride-123")
	}
	if envelope.OccurredAt.Before(before) || envelope.OccurredAt.After(after) {
		t.Errorf("OccurredAt = %v, want timestamp between %v and %v", envelope.OccurredAt, before, after)
	}
	if envelope.OccurredAt.Location() != time.UTC {
		t.Errorf("OccurredAt location = %v, want UTC", envelope.OccurredAt.Location())
	}

	decoded, err := DecodePayload[RideRequested](envelope)
	if err != nil {
		t.Fatalf("DecodePayload() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, payload) {
		t.Errorf("decoded payload = %+v, want %+v", decoded, payload)
	}
}

func TestNewEnvelopeReturnsPayloadMarshalError(t *testing.T) {
	_, err := NewEnvelope("event-1", TypeRideRequested, "rider-service", "ride-123", func() {})
	if err == nil {
		t.Fatal("NewEnvelope() error = nil, want payload marshal error")
	}
	if !strings.Contains(err.Error(), "marshal event payload") {
		t.Fatalf("NewEnvelope() error = %q, want payload marshal context", err)
	}
}

func TestDecodeEnvelopeAcceptsRedisStringAndBytes(t *testing.T) {
	wantPayload := RideAssigned{
		RideID:       "ride-123",
		RiderID:      "rider-456",
		DriverID:     "driver-789",
		DistanceKM:   2.5,
		ETASeconds:   281,
		AssignmentID: "assignment-1",
	}
	payload, err := json.Marshal(wantPayload)
	if err != nil {
		t.Fatalf("json.Marshal(payload) error = %v", err)
	}
	want := Envelope{
		ID:            "event-1",
		Type:          TypeRideAssigned,
		Source:        "dispatch-service",
		CorrelationID: "ride-123",
		OccurredAt:    time.Date(2026, time.September, 4, 12, 30, 0, 0, time.UTC),
		Payload:       payload,
	}
	body, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("json.Marshal(envelope) error = %v", err)
	}

	tests := map[string]any{
		"string": string(body),
		"bytes":  body,
	}
	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := DecodeEnvelope(redis.XMessage{
				ID:     "1700000000000-0",
				Values: map[string]any{"event": value},
			})
			if err != nil {
				t.Fatalf("DecodeEnvelope() error = %v", err)
			}
			if got.ID != want.ID || got.Type != want.Type || got.Source != want.Source || got.CorrelationID != want.CorrelationID {
				t.Errorf("decoded metadata = %+v, want %+v", got, want)
			}
			if !got.OccurredAt.Equal(want.OccurredAt) {
				t.Errorf("OccurredAt = %v, want %v", got.OccurredAt, want.OccurredAt)
			}
			gotPayload, err := DecodePayload[RideAssigned](got)
			if err != nil {
				t.Fatalf("DecodePayload() error = %v", err)
			}
			if !reflect.DeepEqual(gotPayload, wantPayload) {
				t.Errorf("decoded payload = %+v, want %+v", gotPayload, wantPayload)
			}
		})
	}
}

func TestDecodeEnvelopeRejectsInvalidMessages(t *testing.T) {
	tests := []struct {
		name       string
		message    redis.XMessage
		wantErrSub string
	}{
		{
			name:       "missing event field",
			message:    redis.XMessage{ID: "1700000000000-0", Values: map[string]any{}},
			wantErrSub: "1700000000000-0 missing event field",
		},
		{
			name:       "unsupported event type",
			message:    redis.XMessage{ID: "1700000000001-0", Values: map[string]any{"event": 42}},
			wantErrSub: "unexpected event field type int",
		},
		{
			name:       "malformed envelope",
			message:    redis.XMessage{ID: "1700000000002-0", Values: map[string]any{"event": "{not-json"}},
			wantErrSub: "decode event envelope",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeEnvelope(test.message)
			if err == nil {
				t.Fatal("DecodeEnvelope() error = nil, want error")
			}
			if !strings.Contains(err.Error(), test.wantErrSub) {
				t.Fatalf("DecodeEnvelope() error = %q, want substring %q", err, test.wantErrSub)
			}
		})
	}
}

func TestDecodePayloadReturnsTypeError(t *testing.T) {
	envelope := Envelope{Payload: json.RawMessage(`{"eta_seconds":"soon"}`)}

	_, err := DecodePayload[RideAssigned](envelope)
	if err == nil {
		t.Fatal("DecodePayload() error = nil, want JSON type error")
	}
}

func TestPublishReturnsEnvelopeMarshalErrorBeforeRedisCall(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = rdb.Close() })
	envelope := Envelope{Payload: json.RawMessage(`{`)}

	_, err := Publish(context.Background(), rdb, StreamRideRequests, envelope)
	if err == nil {
		t.Fatal("Publish() error = nil, want envelope marshal error")
	}
	if !strings.Contains(err.Error(), "marshal event envelope") {
		t.Fatalf("Publish() error = %q, want envelope marshal context", err)
	}
}
