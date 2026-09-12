package com.metroride.fare.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The delay table of {@code retryBackoff} in {@code shared/pkg/outbox/outbox.go}, computed by hand
 * from that function with its constants (250ms initial, 30s cap): the Java relay must schedule
 * the same retry times as the Go relays.
 */
class RetryBackoffTest {

    private static final Duration INITIAL = Duration.ofMillis(250);
    private static final Duration MAX = Duration.ofSeconds(30);

    @ParameterizedTest(name = "after {0} attempts: {1}ms")
    @CsvSource({
            "0, 250",
            "1, 500",
            "2, 1000",
            "3, 2000",
            "4, 4000",
            "5, 8000",
            "6, 16000",
            "7, 30000",
            "8, 30000",
            "100, 30000"})
    void matchesTheGoTable(int previousAttempts, long expectedMillis) {
        assertThat(RetryBackoff.delay(previousAttempts, INITIAL, MAX)).isEqualTo(Duration.ofMillis(expectedMillis));
    }

    @Test
    void neverExceedsTheCapEvenWhenTheInitialDelayIsLarge() {
        assertThat(RetryBackoff.delay(0, Duration.ofSeconds(45), MAX)).isEqualTo(MAX);
        assertThat(RetryBackoff.delay(3, Duration.ofSeconds(20), MAX)).isEqualTo(MAX);
    }
}
