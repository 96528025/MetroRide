package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	StreamRideRequests      = "events.ride.requests"
	StreamDriverLocations   = "events.driver.locations"
	StreamRideAssignments   = "events.ride.assignments"
	StreamRideNotifications = "events.ride.notifications"
	StreamRideCancellations = "events.ride.cancellations"
	StreamRideCompletions   = "events.ride.completions"
	StreamTrafficUpdates    = "events.traffic.updates"
	StreamDeadLetter        = "events.dead_letter"
	StreamRideFares         = "events.ride.fares"
)

const (
	TypeRideRequested         = "ride_requested"
	TypeDriverLocationUpdated = "driver_location_updated"
	TypeRideAssigned          = "ride_assigned"
	TypeRideCancelled         = "ride_cancelled"
	TypeRideCompleted         = "ride_completed"
	TypeTrafficUpdated        = "traffic_updated"
	TypeNotificationCreated   = "notification_created"
	TypeFareSettled           = "fare_settled"
)

type Envelope struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Source        string          `json:"source"`
	CorrelationID string          `json:"correlation_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
}

type RideRequested struct {
	RideID      string  `json:"ride_id"`
	RiderID     string  `json:"rider_id"`
	PickupLat   float64 `json:"pickup_lat"`
	PickupLng   float64 `json:"pickup_lng"`
	DropoffLat  float64 `json:"dropoff_lat"`
	DropoffLng  float64 `json:"dropoff_lng"`
	RequestedAt string  `json:"requested_at"`
}

type DriverLocationUpdated struct {
	DriverID  string  `json:"driver_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Available bool    `json:"available"`
	UpdatedAt string  `json:"updated_at"`
}

type RideAssigned struct {
	SchemaVersion       int      `json:"schema_version"`
	TripDistanceKM      *float64 `json:"trip_distance_km,omitempty"`
	TripDurationSeconds *float64 `json:"trip_duration_seconds,omitempty"`
	RouteProvider       string   `json:"route_provider,omitempty"`
	RouteCalculatedAt   string   `json:"route_calculated_at,omitempty"`
	RideID              string   `json:"ride_id"`
	RiderID             string   `json:"rider_id"`
	DriverID            string   `json:"driver_id"`
	DistanceKM          float64  `json:"distance_km"`
	ETASeconds          int      `json:"eta_seconds"`
	AssignmentID        string   `json:"assignment_id"`
}

// RideCompleted is published by rider-service when a ride moves from
// "assigned" to "completed". CompletedAt is an RFC 3339 string with
// nanoseconds, like RequestedAt on RideRequested.
type RideCompleted struct {
	RideID       string `json:"ride_id"`
	RiderID      string `json:"rider_id"`
	DriverID     string `json:"driver_id"`
	AssignmentID string `json:"assignment_id"`
	CompletedAt  string `json:"completed_at"`
}

type TrafficUpdated struct {
	Region     string  `json:"region"`
	Congestion float64 `json:"congestion"`
	UpdatedAt  string  `json:"updated_at"`
}

// FareSettled is published by fare-service (Java) to StreamRideFares once a
// ride's quote hold has been reversed and settled. Amounts and the share are
// decimal strings with two (share: as configured) decimal places, never floats,
// so no consumer turns money into floating point by accident. Nothing in this
// repository publishes or consumes it in Go; the struct pins the contract.
type FareSettled struct {
	RideID            string `json:"ride_id"`
	RiderID           string `json:"rider_id"`
	DriverID          string `json:"driver_id"`
	AssignmentID      string `json:"assignment_id"`
	SettlementEventID string `json:"settlement_event_id"`
	Quote             string `json:"quote"`
	DriverAmount      string `json:"driver_amount"`
	PlatformAmount    string `json:"platform_amount"`
	DriverShare       string `json:"driver_share"`
	SettledAt         string `json:"settled_at"`
}

type DeadLetter struct {
	OriginalStream    string         `json:"original_stream,omitempty"`
	OriginalValues    map[string]any `json:"original_values,omitempty"`
	OriginalEventID   string         `json:"original_event_id"`
	OriginalEventType string         `json:"original_event_type"`
	RideID            string         `json:"ride_id,omitempty"`
	Error             string         `json:"error"`
	Service           string         `json:"service"`
	FailedAt          string         `json:"failed_at"`
}

func Publish(ctx context.Context, rdb *redis.Client, stream string, envelope Envelope) (string, error) {
	body, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("marshal event envelope: %w", err)
	}
	id, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"event": string(body)},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("publish event to %s: %w", stream, err)
	}
	return id, nil
}

func NewEnvelope(id, eventType, source, correlationID string, payload any) (Envelope, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal event payload: %w", err)
	}
	return Envelope{
		ID:            id,
		Type:          eventType,
		Source:        source,
		CorrelationID: correlationID,
		OccurredAt:    time.Now().UTC(),
		Payload:       body,
	}, nil
}

func DecodeEnvelope(message redis.XMessage) (Envelope, error) {
	raw, ok := message.Values["event"]
	if !ok {
		return Envelope{}, fmt.Errorf("redis stream message %s missing event field", message.ID)
	}
	var data []byte
	switch value := raw.(type) {
	case string:
		data = []byte(value)
	case []byte:
		data = value
	default:
		return Envelope{}, fmt.Errorf("unexpected event field type %T", raw)
	}
	var envelope Envelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return Envelope{}, fmt.Errorf("decode event envelope: %w", err)
	}
	return envelope, nil
}

func DecodePayload[T any](envelope Envelope) (T, error) {
	var payload T
	err := json.Unmarshal(envelope.Payload, &payload)
	return payload, err
}

// RideCancelled permits an empty assignment for rides canceled before dispatch.
type RideCancelled struct {
	RideID       string `json:"ride_id"`
	RiderID      string `json:"rider_id"`
	DriverID     string `json:"driver_id,omitempty"`
	AssignmentID string `json:"assignment_id,omitempty"`
	CancelledAt  string `json:"cancelled_at"`
}

// NewDeadLetter preserves the source entry for inspection or deliberate replay.
func NewDeadLetter(service, stream string, message redis.XMessage, cause error) DeadLetter {
	p := DeadLetter{OriginalStream: stream, OriginalValues: message.Values, OriginalEventID: message.ID,
		OriginalEventType: "decode_failed", Error: cause.Error(), Service: service, FailedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if env, err := DecodeEnvelope(message); err == nil {
		p.OriginalEventID = env.ID
		p.OriginalEventType = env.Type
		p.RideID = env.CorrelationID
	}
	return p
}
