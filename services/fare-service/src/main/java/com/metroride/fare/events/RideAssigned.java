package com.metroride.fare.events;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Payload of a {@code ride_assigned} envelope, mirroring {@code events.RideAssigned} in
 * {@code shared/pkg/events/events.go}. Nothing in this service reads it yet; it exists so the
 * contract is pinned by a test before fare calculation depends on it.
 */
public record RideAssigned(
        @JsonProperty("ride_id") String rideId,
        @JsonProperty("rider_id") String riderId,
        @JsonProperty("driver_id") String driverId,
        @JsonProperty("distance_km") double distanceKm,
        @JsonProperty("eta_seconds") int etaSeconds,
        @JsonProperty("assignment_id") String assignmentId) {
}
