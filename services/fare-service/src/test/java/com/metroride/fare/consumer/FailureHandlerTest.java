package com.metroride.fare.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.consumer.FailureHandler.Disposition;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeDecodeException;
import com.metroride.fare.pricing.FareQuoteException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.boot.actuate.health.Status;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.jdbc.BadSqlGrammarException;

/**
 * The guarantees that hold the recovery design together, proved against a mocked Redis so the
 * cases an integration test cannot arrange (a dead-letter {@code XADD} that fails while the rest of
 * Redis works, an {@code XACK} that fails after a confirmed dead letter) are covered:
 *
 * <ul>
 *   <li>no {@code XACK} unless the dead-letter {@code XADD} was confirmed;</li>
 *   <li>{@code XACK} only after the {@code XADD}, never before;</li>
 *   <li>an {@code XACK} that fails leaves the entry pending, with the dead letter already counted
 *       (so a duplicate is expected later);</li>
 *   <li>a retryable failure is left pending until the delivery cap, then dead-lettered;</li>
 *   <li>a fatal failure neither acknowledges nor dead-letters, and halts the consumer.</li>
 * </ul>
 */
class FailureHandlerTest {

    private static final String STREAM = "events.ride.assignments";
    private static final String GROUP = "fare-service";
    private static final String MESSAGE_ID = "1788642754475-0";
    private static final int MAX_DELIVERIES = 3;

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final DeadLetterPublisher publisher = mock(DeadLetterPublisher.class);
    private final ConsumerHalt halt = new ConsumerHalt(registry);
    @SuppressWarnings("unchecked")
    private final RedisCommands<String, String> commands = mock(RedisCommands.class);
    private FailureHandler handler;

    @BeforeEach
    void handler() {
        ConsumerProperties properties = new ConsumerProperties(STREAM, GROUP, "fare-service-1", 10,
                Duration.ofSeconds(1), Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(5), MAX_DELIVERIES);
        handler = new FailureHandler(properties, publisher, halt,
                Clock.fixed(Instant.parse("2026-09-08T14:03:07Z"), ZoneOffset.UTC), registry);
    }

    @Test
    void poisonIsDeadLetteredThenAcknowledgedInThatOrder() {
        when(publisher.publish(eq(commands), any(), any(), any())).thenReturn(true);

        Disposition disposition = handler.onFailure(commands, message(), null, decodeFailure(), 1);

        assertThat(disposition).isEqualTo(Disposition.DEAD_LETTERED);
        InOrder order = inOrder(publisher, commands);
        order.verify(publisher).publish(eq(commands), eq(message()), eq(null), any(EnvelopeDecodeException.class));
        order.verify(commands).xack(STREAM, GROUP, MESSAGE_ID);
        assertThat(deadLetters("poison")).isEqualTo(1);
        assertThat(publishFailures()).isZero();
        assertThat(consumeErrors()).isEqualTo(1);
    }

    @Test
    void noAcknowledgementWhenTheDeadLetterWasNotConfirmed() {
        when(publisher.publish(eq(commands), any(), any(), any())).thenReturn(false);

        Disposition disposition = handler.onFailure(commands, message(), null, decodeFailure(), 1);

        assertThat(disposition).isEqualTo(Disposition.LEFT_PENDING);
        verify(commands, never()).xack(anyString(), anyString(), any());
        assertThat(deadLetters("poison")).isZero();
        assertThat(publishFailures()).isEqualTo(1);
        assertThat(redisErrors()).isEqualTo(1);
    }

    @Test
    void aFailedAcknowledgementAfterAConfirmedDeadLetterLeavesTheEntryPending() {
        when(publisher.publish(eq(commands), any(), any(), any())).thenReturn(true);
        when(commands.xack(STREAM, GROUP, MESSAGE_ID)).thenThrow(new RedisCommandTimeoutException("timed out"));

        Disposition disposition = handler.onFailure(commands, message(), null, decodeFailure(), 1);

        // The dead letter is on the stream and counted; the entry's next delivery publishes a
        // second one, which is the documented duplicate.
        assertThat(disposition).isEqualTo(Disposition.LEFT_PENDING);
        assertThat(deadLetters("poison")).isEqualTo(1);
        assertThat(redisErrors()).isEqualTo(1);
    }

    @Test
    void aRetryableFailureBelowTheCapIsLeftPendingWithoutTouchingRedis() {
        Disposition disposition = handler.onFailure(commands, message(), envelope(), lockWait(), MAX_DELIVERIES - 1);

        assertThat(disposition).isEqualTo(Disposition.LEFT_PENDING);
        verifyNoInteractions(publisher);
        verifyNoInteractions(commands);
        assertThat(postgresErrors()).isEqualTo(1);
        assertThat(deadLetters("max_deliveries_reached")).isZero();
    }

    @Test
    void aRetryableFailureOnTheCappedDeliveryIsDeadLettered() {
        when(publisher.publish(eq(commands), any(), any(), any())).thenReturn(true);

        Disposition disposition = handler.onFailure(commands, message(), envelope(), lockWait(), MAX_DELIVERIES);

        assertThat(disposition).isEqualTo(Disposition.DEAD_LETTERED);
        verify(publisher).publish(eq(commands), eq(message()), eq(envelope()), any(QueryTimeoutException.class));
        verify(commands).xack(STREAM, GROUP, MESSAGE_ID);
        assertThat(deadLetters("max_deliveries_reached")).isEqualTo(1);
        assertThat(deadLetters("poison")).isZero();
    }

    @Test
    void anUnknownDeliveryCountNeverExhaustsTheCap() {
        Disposition disposition = handler.onFailure(commands, message(), envelope(), lockWait(), -1);

        assertThat(disposition).isEqualTo(Disposition.LEFT_PENDING);
        verifyNoInteractions(publisher);
    }

    @Test
    void aFatalFailureHaltsWithoutAcknowledgingOrDeadLettering() {
        BadSqlGrammarException wrongSchema = new BadSqlGrammarException(
                "insert", "insert into fare.journal_entries ...", new SQLException("column kind does not exist"));

        Disposition disposition = handler.onFailure(commands, message(), envelope(), wrongSchema, 1);

        assertThat(disposition).isEqualTo(Disposition.HALT);
        verifyNoInteractions(publisher);
        verifyNoInteractions(commands);
        assertThat(halt.isHalted()).isTrue();
        assertThat(halt.reason()).hasValueSatisfying(reason -> assertThat(reason)
                .contains(STREAM + "/" + MESSAGE_ID)
                .contains("column kind does not exist"));
        assertThat(halt.health().getStatus()).isEqualTo(Status.DOWN);
        assertThat(registry.get("metroride.fare.consumer.halted").gauge().value()).isEqualTo(1);
        assertThat(postgresErrors()).isEqualTo(1);
    }

    @Test
    void theFirstHaltReasonIsKept() {
        halt.halt("first");
        halt.halt("second");

        assertThat(halt.reason()).contains("first");
    }

    @Test
    void aQuoteFailureIsCountedUnderItsReason() {
        when(publisher.publish(eq(commands), any(), any(), any())).thenReturn(true);

        handler.onFailure(commands, message(), envelope(),
                new FareQuoteException(FareQuoteException.Reason.CALCULATION, "negative", null), 1);

        assertThat(registry.get("metroride.fare.quote.failures").tag("reason", "calculation").counter().count())
                .isEqualTo(1);
        assertThat(deadLetters("poison")).isEqualTo(1);
    }

    @Test
    void acknowledgeReportsWhetherRedisConfirmed() {
        assertThat(handler.acknowledge(commands, message())).isTrue();
        verify(commands).xack(STREAM, GROUP, MESSAGE_ID);

        when(commands.xack(STREAM, GROUP, MESSAGE_ID)).thenThrow(new RedisCommandTimeoutException("timed out"));
        assertThat(handler.acknowledge(commands, message())).isFalse();
        assertThat(redisErrors()).isEqualTo(1);
    }

    private static StreamMessage<String, String> message() {
        return new StreamMessage<>(STREAM, MESSAGE_ID, Map.of("event", "not-json"));
    }

    private static Envelope envelope() {
        return new Envelope("e1", "ride_assigned", "dispatch-service", "r1", Instant.parse("2026-09-08T14:00:00Z"), null);
    }

    private static EnvelopeDecodeException decodeFailure() {
        return new EnvelopeDecodeException("decode event envelope from message " + MESSAGE_ID + ": not json");
    }

    private static QueryTimeoutException lockWait() {
        return new QueryTimeoutException("canceling statement due to user request");
    }

    private double deadLetters(String reason) {
        return registry.get("metroride.fare.dead_letters").tag("stream", STREAM).tag("reason", reason).counter().count();
    }

    private double publishFailures() {
        return registry.get("metroride.fare.dead_letter.publish.failures").tag("stream", STREAM).counter().count();
    }

    private double redisErrors() {
        return registry.get("metroride.dependency.errors").tag("dependency", "redis").counter().count();
    }

    private double postgresErrors() {
        return registry.get("metroride.dependency.errors").tag("dependency", "postgres").counter().count();
    }

    private double consumeErrors() {
        return registry.get("metroride.stream.consume.errors").tag("stream", STREAM).counter().count();
    }
}
