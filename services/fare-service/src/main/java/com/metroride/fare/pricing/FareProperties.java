package com.metroride.fare.pricing;

import java.math.BigDecimal;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Fare rates, bound from the {@code metroride.fare.*} keys in {@code application.yml}. They are
 * exact decimals, never doubles, so a rate such as {@code 0.30} is the number the operator wrote.
 *
 * @param baseFare    charged on every ride regardless of distance or time
 * @param perKm       charged per kilometre of {@code distance_km}
 * @param perMinute   charged per minute of {@code eta_seconds}
 * @param driverShare fraction of a settled fare that goes to the driver, between 0 and 1; bound
 *                    now so the rate card is complete, read only once settlement exists
 */
@ConfigurationProperties(prefix = "metroride.fare")
public record FareProperties(BigDecimal baseFare, BigDecimal perKm, BigDecimal perMinute, BigDecimal driverShare) {

    public FareProperties {
        requireNonNegative("metroride.fare.base-fare", baseFare);
        requireNonNegative("metroride.fare.per-km", perKm);
        requireNonNegative("metroride.fare.per-minute", perMinute);
        requireNonNegative("metroride.fare.driver-share", driverShare);
        if (driverShare.compareTo(BigDecimal.ONE) > 0) {
            throw new IllegalArgumentException("metroride.fare.driver-share must be at most 1, got " + driverShare);
        }
    }

    private static void requireNonNegative(String key, BigDecimal value) {
        if (value == null) {
            throw new IllegalArgumentException(key + " is required");
        }
        if (value.signum() < 0) {
            throw new IllegalArgumentException(key + " must not be negative, got " + value);
        }
    }
}
