package com.metroride.fare.events;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;

/** Version 2 distinguishes driver approach from the passenger's road route. */
public record RideAssigned(
        @JsonProperty("ride_id") String rideId,
        @JsonProperty("rider_id") String riderId,
        @JsonProperty("driver_id") String driverId,
        @JsonProperty("distance_km") double distanceKm,
        @JsonProperty("eta_seconds") int etaSeconds,
        @JsonProperty("assignment_id") String assignmentId,
        @JsonProperty("schema_version") Integer schemaVersion,
        @JsonProperty("trip_distance_km") BigDecimal tripDistanceKm,
        @JsonProperty("trip_duration_seconds") BigDecimal tripDurationSeconds,
        @JsonProperty("route_provider") String routeProvider,
        @JsonProperty("route_calculated_at") String routeCalculatedAt) {
    /** Decoding a historical event remains possible; it cannot create a new fare hold. */
    public RideAssigned(String rideId, String riderId, String driverId, double distanceKm,
                        int etaSeconds, String assignmentId) {
        this(rideId, riderId, driverId, distanceKm, etaSeconds, assignmentId, null, null, null, null, null);
    }
}
