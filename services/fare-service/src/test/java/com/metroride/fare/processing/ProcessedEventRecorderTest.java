package com.metroride.fare.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.metroride.fare.consumer.FailureClass;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.ledger.JournalEntry;
import com.metroride.fare.ledger.JournalKind;
import com.metroride.fare.ledger.LedgerConflictException;
import com.metroride.fare.ledger.LedgerRepository;
import com.metroride.fare.ledger.Money;
import com.metroride.fare.ledger.StoredJournalEntry;
import com.metroride.fare.outbox.OutboxRepository;
import com.metroride.fare.pricing.FareCalculator;
import com.metroride.fare.pricing.FareProperties;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

/**
 * The recorder's branches that no integration test can reach once the V3 unique indexes exist:
 * they are exercised against a mocked ledger, because the real database refuses the states they
 * guard against. The recorder is built by hand (no Spring, no transaction); what is tested is
 * what it does with the rows it is handed.
 */
class ProcessedEventRecorderTest {

    private static final Instant NOW = Instant.parse("2026-09-11T10:00:00Z");
    private static final String RIDE = "ride-1";

    private final ProcessedEventRepository events = mock(ProcessedEventRepository.class);
    private final LedgerRepository ledger = mock(LedgerRepository.class);
    private final ObjectMapper mapper = Jackson2ObjectMapperBuilder.json().build();
    private final EnvelopeCodec codec = new EnvelopeCodec(mapper);
    private final OutboxRepository outbox = mock(OutboxRepository.class);
    private final FareProperties rates = new FareProperties(
            new BigDecimal("2.50"), new BigDecimal("1.20"), new BigDecimal("0.30"), new BigDecimal("0.80"));
    private final RideContextRepository context = mock(RideContextRepository.class);
    private ProcessedEventRecorder recorder;

    @BeforeEach
    void recorder() {
        when(events.insertIfAbsent(anyString(), anyString(), anyString(), any())).thenReturn(1);
        recorder = new ProcessedEventRecorder(events, ledger, codec, new FareCalculator(rates), rates,
                outbox, mapper, Clock.fixed(NOW, ZoneOffset.UTC), context);
    }

    /**
     * A settlement enqueues exactly one {@code fare_settled} for {@code events.ride.fares}, with the
     * figures of the settlement entry it was written with: the quote, the driver's rounded share,
     * the platform's remainder, and the completion event's ID as {@code settlement_event_id}.
     */
    @Test
    void aSettlementEnqueuesOneFareSettledEventWithTheLedgerFigures() {
        when(ledger.lockQuoteHolds(RIDE)).thenReturn(List.of(
                new StoredJournalEntry(1, NOW, JournalEntry.quoteHold(RIDE, "assigned-1", Money.of("5.85")))));
        when(ledger.hasSettlement(RIDE)).thenReturn(false);
        when(ledger.append(any(), any())).thenReturn(7L);

        recorder.record("events.ride.completions", completion("completed-1"));

        ArgumentCaptor<Envelope> envelope = ArgumentCaptor.forClass(Envelope.class);
        verify(outbox).enqueue(eq(Envelope.STREAM_RIDE_FARES), envelope.capture(), eq(NOW));
        Envelope published = envelope.getValue();
        assertThat(published.type()).isEqualTo(Envelope.TYPE_FARE_SETTLED);
        assertThat(published.source()).isEqualTo("fare-service");
        assertThat(published.correlationId()).isEqualTo(RIDE);
        assertThat(published.id()).isNotEqualTo("completed-1");
        assertThat(published.occurredAt()).isEqualTo(NOW);
        JsonNode payload = published.payload();
        assertThat(payload.get("ride_id").asText()).isEqualTo(RIDE);
        assertThat(payload.get("rider_id").asText()).isEqualTo("rider-42");
        assertThat(payload.get("driver_id").asText()).isEqualTo("driver-2");
        assertThat(payload.get("assignment_id").asText()).isEqualTo("a-1");
        assertThat(payload.get("settlement_event_id").asText()).isEqualTo("completed-1");
        assertThat(payload.get("quote").asText()).isEqualTo("5.85");
        assertThat(payload.get("driver_amount").asText()).isEqualTo("4.68");
        assertThat(payload.get("platform_amount").asText()).isEqualTo("1.17");
        assertThat(payload.get("driver_share").asText()).isEqualTo("0.80");
        assertThat(payload.get("settled_at").asText()).isEqualTo(NOW.toString());
    }

    /** Nothing is enqueued for a settlement that did not happen. */
    @Test
    void aRefusedSettlementEnqueuesNothing() {
        when(ledger.lockQuoteHolds(RIDE)).thenReturn(List.of());

        assertThatThrownBy(() -> recorder.record("events.ride.completions", completion("completed-1")))
                .isInstanceOf(SettlementException.class);

        verify(outbox, never()).enqueue(anyString(), any(), any());
    }

    /**
     * Two holds for one ride: the index should have made this impossible, so the recorder does
     * not pick one and settle; it fails fatally and writes nothing.
     */
    @Test
    void twoHoldsForOneRideAreFatalAndNothingIsWritten() {
        when(ledger.lockQuoteHolds(RIDE)).thenReturn(List.of(
                new StoredJournalEntry(1, NOW, JournalEntry.quoteHold(RIDE, "assigned-1", Money.of("5.85"))),
                new StoredJournalEntry(2, NOW, JournalEntry.quoteHold(RIDE, "assigned-2", Money.of("5.85")))));

        assertThatThrownBy(() -> recorder.record("events.ride.completions", completion("completed-1")))
                .isInstanceOfSatisfying(SettlementException.class, failure -> {
                    assertThat(failure.reason()).isEqualTo(SettlementException.Reason.AMBIGUOUS_HOLD);
                    assertThat(failure.failureClass()).isEqualTo(FailureClass.FATAL);
                    assertThat(failure.getMessage()).contains("2 quote_hold").contains("journal_entries_one_quote_hold_per_ride");
                });
        verify(ledger, never()).append(any(), any());
        verify(ledger, never()).hasSettlement(anyString());
    }

    /**
     * The settlement index fires after the check under the lock said "not settled" (a window two
     * writers can hit): the recorder reports it as {@code already_settled}, so the consumer files
     * it exactly like the checked case, with the index conflict as the cause.
     */
    @Test
    void aSettlementRefusedByTheIndexIsReportedAsAlreadySettled() {
        when(ledger.lockQuoteHolds(RIDE)).thenReturn(List.of(
                new StoredJournalEntry(1, NOW, JournalEntry.quoteHold(RIDE, "assigned-1", Money.of("5.85")))));
        when(ledger.hasSettlement(RIDE)).thenReturn(false);
        LedgerConflictException refused = new LedgerConflictException(
                LedgerConflictException.Conflict.DUPLICATE_SETTLEMENT, "ride ride-1 already has a settlement", null);
        when(ledger.append(any(), any())).thenAnswer(invocation -> {
            JournalEntry entry = invocation.getArgument(0);
            if (entry.kind() == JournalKind.SETTLEMENT) {
                throw refused;
            }
            return 7L;
        });

        assertThatThrownBy(() -> recorder.record("events.ride.completions", completion("completed-2")))
                .isInstanceOfSatisfying(SettlementException.class, failure -> {
                    assertThat(failure.reason()).isEqualTo(SettlementException.Reason.ALREADY_SETTLED);
                    assertThat(failure.getCause()).isSameAs(refused);
                    assertThat(failure.getMessage()).contains("journal_entries_one_settlement_per_ride");
                });
    }

    /** A second hold refused by the index is the assignment event's problem; it passes through as is. */
    @Test
    void aHoldRefusedByTheIndexPropagatesAsALedgerConflict() {
        LedgerConflictException refused = new LedgerConflictException(
                LedgerConflictException.Conflict.DUPLICATE_HOLD, "ride ride-1 already has a quote_hold", null);
        when(ledger.append(any(), any())).thenThrow(refused);

        assertThatThrownBy(() -> recorder.record("events.ride.assignments", assignment("assigned-9")))
                .isSameAs(refused);
        verify(ledger, never()).lockQuoteHolds(anyString());
    }

    private Envelope completion(String eventId) {
        return codec.decode("test", Map.of(EnvelopeCodec.EVENT_FIELD,
                "{\"id\":\"" + eventId + "\",\"type\":\"ride_completed\",\"source\":\"rider-service\","
                        + "\"correlation_id\":\"" + RIDE + "\",\"occurred_at\":\"2026-09-11T10:00:00Z\","
                        + "\"payload\":{\"ride_id\":\"" + RIDE + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                        + "\"assignment_id\":\"a-1\",\"completed_at\":\"2026-09-11T10:00:00Z\"}}"));
    }

    private Envelope assignment(String eventId) {
        return codec.decode("test", Map.of(EnvelopeCodec.EVENT_FIELD,
                "{\"id\":\"" + eventId + "\",\"type\":\"ride_assigned\",\"source\":\"dispatch-service\","
                        + "\"correlation_id\":\"" + RIDE + "\",\"occurred_at\":\"2026-09-11T10:00:00Z\","
                        + "\"payload\":{\"ride_id\":\"" + RIDE + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                        + "\"schema_version\":2,\"route_provider\":\"test-fixture\",\"route_calculated_at\":\"2026-09-11T10:00:00Z\",\"trip_distance_km\":1.8612,\"trip_duration_seconds\":223,\"distance_km\":1.8612,\"eta_seconds\":223,\"assignment_id\":\"a-1\"}}"));
    }
}
