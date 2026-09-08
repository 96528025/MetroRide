package com.metroride.fare.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class StreamEntryAgeTest {

    /** The ID from the captured Go {@code XADD} in {@code EnvelopeCodecTest}: 2026-09-05T21:12:34.475Z. */
    private static final String GO_MESSAGE_ID = "1788642754475-0";
    private static final Instant ADDED = Instant.ofEpochMilli(1788642754475L);

    @Test
    void readsTheMillisecondTimestampBeforeTheSequenceSuffix() {
        assertThat(StreamEntryAge.timestampOf(GO_MESSAGE_ID)).contains(ADDED);
        assertThat(StreamEntryAge.of(GO_MESSAGE_ID, ADDED.plusSeconds(90))).contains(Duration.ofSeconds(90));
    }

    @Test
    void theSequenceSuffixDoesNotMatter() {
        assertThat(StreamEntryAge.of("1788642754475-7", ADDED.plusSeconds(3))).contains(Duration.ofSeconds(3));
        assertThat(StreamEntryAge.of("1788642754475-18446744073709551615", ADDED.plusMillis(250)))
                .contains(Duration.ofMillis(250));
    }

    @Test
    void aBareTimestampWithoutSuffixIsAccepted() {
        assertThat(StreamEntryAge.of("1788642754475", ADDED.plusSeconds(1))).contains(Duration.ofSeconds(1));
    }

    @Test
    void anEntryFromTheFutureHasAgeZeroNotNegative() {
        assertThat(StreamEntryAge.of(GO_MESSAGE_ID, ADDED.minusSeconds(5))).contains(Duration.ZERO);
    }

    @Test
    void anIdWithoutATimestampHasNoAge() {
        assertThat(StreamEntryAge.of(null, ADDED)).isEmpty();
        assertThat(StreamEntryAge.of("", ADDED)).isEmpty();
        assertThat(StreamEntryAge.of("-0", ADDED)).isEmpty();
        assertThat(StreamEntryAge.of("abc-0", ADDED)).isEmpty();
        assertThat(StreamEntryAge.of("17886427544750000000000-0", ADDED)).as("overflows a long").isEmpty();
        assertThat(StreamEntryAge.of("1788642754475-", ADDED)).as("trailing dash is still a timestamp")
                .contains(Duration.ZERO);
    }
}
