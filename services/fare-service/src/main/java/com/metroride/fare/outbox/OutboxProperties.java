package com.metroride.fare.outbox;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Relay settings for {@code fare.event_outbox}, bound from {@code metroride.outbox.*} in
 * {@code application.yml}. The defaults are the constants of {@code shared/pkg/outbox} in the Go
 * services, so the three relays in the system behave alike.
 *
 * @param pollInterval    how often the relay thread looks for publishable rows ({@code defaultPollInterval})
 * @param batchSize       rows taken per pass with {@code for update skip locked} ({@code defaultBatchSize})
 * @param maxRetryBackoff cap on the delay between attempts of one failed row ({@code maxRetryBackoff})
 */
@ConfigurationProperties(prefix = "metroride.outbox")
public record OutboxProperties(Duration pollInterval, int batchSize, Duration maxRetryBackoff) {

    public OutboxProperties {
        if (pollInterval == null || pollInterval.isZero() || pollInterval.isNegative()) {
            throw new IllegalArgumentException("metroride.outbox.poll-interval must be positive, got " + pollInterval);
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("metroride.outbox.batch-size must be at least 1, got " + batchSize);
        }
        if (maxRetryBackoff == null || maxRetryBackoff.compareTo(pollInterval) < 0) {
            throw new IllegalArgumentException("metroride.outbox.max-retry-backoff must be at least the poll interval, got "
                    + maxRetryBackoff);
        }
    }
}
