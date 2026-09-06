package com.metroride.fare.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Redis Streams consumer settings, bound from the {@code metroride.consumer.*} keys in
 * {@code application.yml}. {@code group} and {@code name} come from {@code CONSUMER_GROUP} and
 * {@code CONSUMER_NAME}, the same variables the Go consumers read.
 *
 * @param stream       stream to consume; {@code events.ride.assignments}, see {@code shared/pkg/events/events.go}
 * @param group        consumer group name
 * @param name         this consumer's name inside the group
 * @param batchSize    {@code COUNT} for each {@code XREADGROUP}
 * @param blockTimeout {@code BLOCK} for each {@code XREADGROUP}; must be shorter than the Redis command timeout
 * @param errorBackoff pause after a failed read before the next attempt, so an outage does not spin the loop
 */
@ConfigurationProperties(prefix = "metroride.consumer")
public record ConsumerProperties(
        String stream,
        String group,
        String name,
        int batchSize,
        Duration blockTimeout,
        Duration errorBackoff) {
}
