package com.metroride.fare.consumer;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Age of a Redis Stream entry, derived from its ID. An auto-generated stream ID is
 * {@code <milliseconds since the epoch>-<sequence>}, so the ID itself records when the producer's
 * {@code XADD} ran; no extra bookkeeping is needed to know how long an entry has been waiting.
 *
 * <p>The age is logged with every reclaimed and every failed entry and takes part in no decision.
 * The retry budget is the delivery count ({@code max-deliveries}): age counts time the service may
 * have spent stopped, so an age budget would give up on exactly the backlog a restart exists to
 * work through.
 */
public final class StreamEntryAge {

    private StreamEntryAge() {
    }

    /**
     * The millisecond timestamp encoded in a stream ID, or empty when the ID is not of the
     * {@code <ms>-<seq>} (or bare {@code <ms>}) form Redis generates for {@code XADD ... *}.
     */
    public static Optional<Instant> timestampOf(String messageId) {
        if (messageId == null || messageId.isEmpty()) {
            return Optional.empty();
        }
        int dash = messageId.indexOf('-');
        String millis = dash < 0 ? messageId : messageId.substring(0, dash);
        if (millis.isEmpty() || !millis.chars().allMatch(Character::isDigit)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Instant.ofEpochMilli(Long.parseLong(millis)));
        } catch (NumberFormatException | java.time.DateTimeException e) {
            return Optional.empty();
        }
    }

    /**
     * {@code now} minus the ID's timestamp, never negative, or empty when the ID carries no
     * timestamp (logged as {@code -1}).
     */
    public static Optional<Duration> of(String messageId, Instant now) {
        return timestampOf(messageId).map(added -> {
            Duration age = Duration.between(added, now);
            return age.isNegative() ? Duration.ZERO : age;
        });
    }
}
