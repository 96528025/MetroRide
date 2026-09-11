package com.metroride.fare.config;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Redis Streams consumer settings, bound from the {@code metroride.consumer.*} keys in
 * {@code application.yml}. {@code group} and {@code name} come from {@code CONSUMER_GROUP} and
 * {@code CONSUMER_NAME}, the same variables the Go consumers read.
 *
 * @param streams         streams to consume, all through the same group and consumer name and one
 *                        {@code XREADGROUP}: {@code events.ride.assignments} and
 *                        {@code events.ride.completions}, see {@code shared/pkg/events/events.go}.
 *                        Order is not significant to the consumer (envelopes are dispatched by
 *                        type, not by stream); {@code application.yml} lists the assignments stream
 *                        first and the completions stream second, and the integration tests rely
 *                        on that order to know which stream to publish on
 * @param group           consumer group name
 * @param name            this consumer's name inside the group
 * @param batchSize       {@code COUNT} for each {@code XREADGROUP} and each {@code XAUTOCLAIM}
 * @param blockTimeout    {@code BLOCK} for each {@code XREADGROUP}; must be shorter than the Redis command timeout
 * @param errorBackoff    pause after a failed read before the next attempt, so an outage does not spin the loop
 * @param reclaimInterval how often the consumer thread runs one {@code XAUTOCLAIM} before its next read
 * @param reclaimMinIdle  {@code min-idle-time} of that {@code XAUTOCLAIM}: how long a pending entry must
 *                        have gone without a delivery before it is delivered again
 * @param maxDeliveries   a retryable failure on this delivery of an entry dead-letters it; Redis counts
 *                        deliveries in the pending entry list, and consecutive deliveries are at least
 *                        {@code reclaimMinIdle} apart, so this guarantees a minimum retry window since
 *                        the first delivery
 */
@ConfigurationProperties(prefix = "metroride.consumer")
public record ConsumerProperties(
        List<String> streams,
        String group,
        String name,
        int batchSize,
        Duration blockTimeout,
        Duration errorBackoff,
        Duration reclaimInterval,
        Duration reclaimMinIdle,
        int maxDeliveries) {
}
