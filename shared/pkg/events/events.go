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
	StreamTrafficUpdates    = "events.traffic.updates"
)

const (
	TypeRideRequested         = "ride_requested"
	TypeDriverLocationUpdated = "driver_location_updated"
	TypeRideAssigned          = "ride_assigned"
	TypeRideCompleted         = "ride_completed"
	TypeTrafficUpdated        = "traffic_updated"
	TypeNotificationCreated   = "notification_created"
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
	RideID       string  `json:"ride_id"`
	RiderID      string  `json:"rider_id"`
	DriverID     string  `json:"driver_id"`
	DistanceKM   float64 `json:"distance_km"`
	ETASeconds   int     `json:"eta_seconds"`
	AssignmentID string  `json:"assignment_id"`
}

type TrafficUpdated struct {
	Region     string  `json:"region"`
	Congestion float64 `json:"congestion"`
	UpdatedAt  string  `json:"updated_at"`
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
