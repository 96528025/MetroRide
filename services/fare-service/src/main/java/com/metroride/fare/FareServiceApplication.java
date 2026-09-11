package com.metroride.fare;

import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.pricing.FareProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * fare-service consumes {@code events.ride.assignments} and {@code events.ride.completions} from
 * Redis Streams, records every envelope it sees exactly once in its own PostgreSQL schema, quotes
 * and holds the fare of each first-seen {@code ride_assigned} in a double-entry ledger, and on
 * {@code ride_completed} reverses that hold and settles the quoted amount between driver and
 * platform.
 *
 * <p>Publication of {@code events.ride.fares} is deliberately absent; see
 * {@code services/fare-service/README.md} for the boundary.
 */
@SpringBootApplication
@EnableConfigurationProperties({ConsumerProperties.class, FareProperties.class})
public class FareServiceApplication {

    /** Value of the {@code service} label on every metric, matching the Go services' label. */
    public static final String SERVICE_NAME = "fare-service";

    public static void main(String[] args) {
        SpringApplication.run(FareServiceApplication.class, args);
    }
}
