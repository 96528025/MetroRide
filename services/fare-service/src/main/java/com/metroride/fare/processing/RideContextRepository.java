package com.metroride.fare.processing;

import com.metroride.fare.events.RideAssigned;
import com.metroride.fare.pricing.FareProperties;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** All calls participate in the recorder's transaction; quote context is insert-only. */
@Repository
public class RideContextRepository {
    private final JdbcClient jdbc;
    public RideContextRepository(JdbcClient jdbc) { this.jdbc = jdbc; }

    public String lock(String rideId) {
        jdbc.sql("insert into fare.ride_state(ride_id) values (?) on conflict do nothing")
                .param(rideId).update();
        return jdbc.sql("select state from fare.ride_state where ride_id = ? for update")
                .param(rideId).query(String.class).single();
    }

    public void mark(String rideId, String state) {
        jdbc.sql("update fare.ride_state set state = ? where ride_id = ?")
                .param(state).param(rideId).update();
    }

    public Optional<BigDecimal> driverShare(String rideId) {
        return jdbc.sql("select driver_share from fare.quote_context where ride_id = ?")
                .param(rideId).query(BigDecimal.class).optional();
    }

    public void save(RideAssigned a, String eventId, FareProperties rates) {
        jdbc.sql("""
                insert into fare.quote_context
                  (ride_id, source_event_id, pricing_version, trip_distance_km, trip_duration_seconds,
                   route_provider, route_calculated_at, base_fare, per_km, per_minute, driver_share)
                values (?, ?, 'passenger-road-v2', ?, ?, ?, ?, ?, ?, ?, ?)
                """)
                .param(a.rideId()).param(eventId).param(a.tripDistanceKm()).param(a.tripDurationSeconds())
                .param(a.routeProvider()).param(Timestamp.from(Instant.parse(a.routeCalculatedAt())))
                .param(rates.baseFare()).param(rates.perKm()).param(rates.perMinute()).param(rates.driverShare())
                .update();
    }
}
