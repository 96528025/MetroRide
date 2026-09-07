package com.metroride.fare;

import com.metroride.fare.config.ConsumerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * fare-service consumes {@code events.ride.assignments} from Redis Streams and records
 * every envelope it sees exactly once in its own PostgreSQL schema.
 *
 * <p>That is the whole service for now. Fare calculation, the ledger, settlement on
 * {@code ride_completed}, and publication of {@code events.ride.fares} are deliberately
 * absent; see {@code services/fare-service/README.md} for the boundary.
 */
@SpringBootApplication
@EnableConfigurationProperties(ConsumerProperties.class)
public class FareServiceApplication {

    /** Value of the {@code service} label on every metric, matching the Go services' label. */
    public static final String SERVICE_NAME = "fare-service";

    public static void main(String[] args) {
        SpringApplication.run(FareServiceApplication.class, args);
    }
}
