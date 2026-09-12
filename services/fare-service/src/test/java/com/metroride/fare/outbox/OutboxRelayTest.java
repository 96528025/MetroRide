package com.metroride.fare.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.metroride.fare.outbox.OutboxRepository.PendingEvent;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.transaction.support.TransactionOperations;

/**
 * The batch semantics of one relay pass against a mocked Redis and repository: a failed
 * {@code XADD} is recorded with the Go backoff and does not stop the batch, a confirmed
 * {@code XADD} is marked published, and a failure to mark aborts the pass so the row is published
 * again (the at-least-once window, by design).
 */
class OutboxRelayTest {

    private final OutboxRepository repository = mock(OutboxRepository.class);
    @SuppressWarnings("unchecked")
    private final RedisCommands<String, String> commands = mock(RedisCommands.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private OutboxRelay relay;

    @BeforeEach
    void relay() {
        OutboxProperties properties = new OutboxProperties(Duration.ofMillis(250), 25, Duration.ofSeconds(30));
        relay = new OutboxRelay(properties, repository, TransactionOperations.withoutTransaction(),
                mock(LettuceConnectionFactory.class), registry, Duration.ofSeconds(10));
    }

    @Test
    void aSlowRedisRecordsTheFailureWithTheGoBackoffAndContinuesTheBatch() {
        PendingEvent slow = new PendingEvent("e1", "events.ride.fares", "{\"id\":\"e1\"}", 3);
        PendingEvent fine = new PendingEvent("e2", "events.ride.fares", "{\"id\":\"e2\"}", 0);
        when(repository.lockPending(25)).thenReturn(List.of(slow, fine));
        when(commands.xadd(eq("events.ride.fares"), eq(Map.of("event", "{\"id\":\"e1\"}"))))
                .thenThrow(new RedisCommandTimeoutException("Command timed out after 2 second(s)"));
        when(commands.xadd(eq("events.ride.fares"), eq(Map.of("event", "{\"id\":\"e2\"}")))).thenReturn("1-0");

        OutboxRelay.Summary summary = relay.publishPending(commands);

        assertThat(summary.failed()).containsExactly(slow);
        assertThat(summary.published()).containsExactly(fine);
        ArgumentCaptor<Double> delay = ArgumentCaptor.forClass(Double.class);
        verify(repository).markFailed(eq(slow), delay.capture(), eq("Command timed out after 2 second(s)"));
        assertThat(delay.getValue()).as("after 3 attempts the Go table says 2s").isEqualTo(2.0);
        verify(repository).markPublished(fine);
        verify(repository, never()).markPublished(slow);
        assertThat(published()).isEqualTo(1);
        assertThat(failures()).isEqualTo(1);
    }

    @Test
    void aRowThatCannotBeMarkedPublishedAbortsThePassAndCountsNothing() {
        PendingEvent first = new PendingEvent("e1", "events.ride.fares", "{\"id\":\"e1\"}", 0);
        PendingEvent second = new PendingEvent("e2", "events.ride.fares", "{\"id\":\"e2\"}", 0);
        when(repository.lockPending(25)).thenReturn(List.of(first, second));
        when(commands.xadd(anyString(), any(Map.class))).thenReturn("1-0");
        doThrow(new QueryTimeoutException("canceling statement due to user request")).when(repository).markPublished(second);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> relay.publishPending(commands))
                .isInstanceOf(QueryTimeoutException.class);

        // Redis has both entries; the transaction that would have recorded the first is gone, so
        // the next pass publishes it again under the same envelope ID.
        verify(commands).xadd(eq("events.ride.fares"), eq(Map.of("event", "{\"id\":\"e1\"}")));
        verify(repository).markPublished(first);
        assertThat(published()).isZero();
        assertThat(failures()).isZero();
    }

    @Test
    void anEmptyBatchTouchesNeitherRedisNorTheCounters() {
        when(repository.lockPending(25)).thenReturn(List.of());

        OutboxRelay.Summary summary = relay.publishPending(commands);

        assertThat(summary.published()).isEmpty();
        assertThat(summary.failed()).isEmpty();
        verify(commands, never()).xadd(anyString(), any(Map.class));
        verify(repository, never()).markFailed(any(), anyDouble(), anyString());
    }

    private double published() {
        return registry.get("metroride.outbox.events.published").tag("stream", "events.ride.fares").counter().count();
    }

    private double failures() {
        return registry.get("metroride.outbox.publish.failures").tag("stream", "events.ride.fares").counter().count();
    }
}
