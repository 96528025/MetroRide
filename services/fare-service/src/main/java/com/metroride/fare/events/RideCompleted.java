package com.metroride.fare.events;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Payload of a {@code ride_completed} envelope, mirroring {@code events.RideCompleted} in
 * {@code shared/pkg/events/events.go}. Published by rider-service through its outbox when a ride
 * moves from {@code assigned} to {@code completed}; consumed here to reverse the quote hold and
 * settle the fare.
 *
 * @param completedAt RFC 3339 string with nanoseconds, as the Go side writes it; kept as text
 *                    because nothing in this service computes with it
 */
public record RideCompleted(
        @JsonProperty("ride_id") String rideId,
        @JsonProperty("rider_id") String riderId,
        @JsonProperty("driver_id") String driverId,
        @JsonProperty("assignment_id") String assignmentId,
        @JsonProperty("completed_at") String completedAt) {
}
