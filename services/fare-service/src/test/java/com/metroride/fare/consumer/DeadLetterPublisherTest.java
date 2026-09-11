package com.metroride.fare.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.events.EnvelopeDecodeException;
import com.metroride.fare.pricing.FareQuoteException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

/**
 * The dead letter this service writes must be indistinguishable in shape from the one
 * {@code publishDeadLetter} in dispatch-service writes, because both land on the same stream and
 * whoever reads it will decode both with {@code events.DeadLetter}.
 */
class DeadLetterPublisherTest {

    /**
     * The JSON tags of {@code events.Envelope} and {@code events.DeadLetter} in
     * {@code shared/pkg/events/events.go}, in struct order. {@code ride_id} carries
     * {@code omitempty}.
     */
    static final List<String> GO_ENVELOPE_FIELDS =
            List.of("id", "type", "source", "correlation_id", "occurred_at", "payload");
    static final List<String> GO_DEAD_LETTER_FIELDS =
            List.of("original_event_id", "original_event_type", "ride_id", "error", "service", "failed_at");

    private static final Instant NOW = Instant.parse("2026-09-08T14:03:07.123456789Z");
    private static final String STREAM = "events.ride.assignments";
    private static final String MESSAGE_ID = "1788642754475-0";

    private final ObjectMapper mapper = Jackson2ObjectMapperBuilder.json().build();
    private final DeadLetterPublisher publisher =
            new DeadLetterPublisher(mapper, new EnvelopeCodec(mapper), Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void writesEveryGoFieldUnderItsGoNameForAQuoteFailure() throws Exception {
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        Envelope original = new EnvelopeCodec(mapper).decode(MESSAGE_ID, Map.of(EnvelopeCodec.EVENT_FIELD,
                "{\"id\":\"" + eventId + "\",\"type\":\"ride_assigned\",\"source\":\"dispatch-service\","
                        + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-05T21:12:34.293710969Z\","
                        + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"distance_km\":-1,\"eta_seconds\":60}}"));
        FareQuoteException cause = new FareQuoteException(FareQuoteException.Reason.CALCULATION,
                "quote for ride " + rideId + ": distance must not be negative", null);

        JsonNode json = mapper.readTree(mapper.writeValueAsString(publisher.envelope(message(), original, cause)));

        assertThat(fieldNames(json)).isEqualTo(GO_ENVELOPE_FIELDS);
        assertThat(UUID.fromString(json.get("id").asText())).isNotNull();
        assertThat(json.get("id").asText()).isNotEqualTo(eventId);
        assertThat(json.get("type").asText()).isEqualTo("dead_lettered");
        assertThat(json.get("source").asText()).isEqualTo("fare-service");
        assertThat(json.get("correlation_id").asText()).isEqualTo(rideId);
        assertThat(Instant.parse(json.get("occurred_at").asText())).isEqualTo(NOW);

        JsonNode payload = json.get("payload");
        assertThat(fieldNames(payload)).isEqualTo(GO_DEAD_LETTER_FIELDS);
        assertThat(payload.get("original_event_id").asText()).isEqualTo(eventId);
        assertThat(payload.get("original_event_type").asText()).isEqualTo("ride_assigned");
        assertThat(payload.get("ride_id").asText()).isEqualTo(rideId);
        assertThat(payload.get("error").asText()).isEqualTo(cause.getMessage());
        assertThat(payload.get("service").asText()).isEqualTo("fare-service");
        // RFC 3339 with fractional seconds and a Z suffix, what Go's time.RFC3339Nano parses.
        assertThat(payload.get("failed_at").asText()).isEqualTo("2026-09-08T14:03:07.123456789Z");
    }

    /** For a {@code ride_completed} the ride ID is read from the payload, as for a {@code ride_assigned}. */
    @Test
    void aCompletionsRideIdIsTakenFromItsPayload() throws Exception {
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        // An empty correlation_id, so the value can only have come from the payload.
        Envelope original = new EnvelopeCodec(mapper).decode(MESSAGE_ID, Map.of(EnvelopeCodec.EVENT_FIELD,
                "{\"id\":\"" + eventId + "\",\"type\":\"ride_completed\",\"source\":\"rider-service\","
                        + "\"correlation_id\":\"\",\"occurred_at\":\"2026-09-10T21:12:34.293710969Z\","
                        + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                        + "\"assignment_id\":\"a1\",\"completed_at\":\"2026-09-10T21:12:34.293710969Z\"}}"));

        JsonNode json = mapper.readTree(mapper.writeValueAsString(publisher.envelope(
                new StreamMessage<>("events.ride.completions", MESSAGE_ID, Map.of(EnvelopeCodec.EVENT_FIELD, "{}")),
                original, new IllegalStateException("ride " + rideId + " is already settled"))));

        assertThat(json.get("correlation_id").asText()).isEqualTo(rideId);
        JsonNode payload = json.get("payload");
        assertThat(payload.get("original_event_id").asText()).isEqualTo(eventId);
        assertThat(payload.get("original_event_type").asText()).isEqualTo("ride_completed");
        assertThat(payload.get("ride_id").asText()).isEqualTo(rideId);
        assertThat(payload.get("error").asText()).contains("already settled");
    }

    @Test
    void anUndecodableEntryIsDeadLetteredUnderItsMessageIdWithoutARideId() throws Exception {
        EnvelopeDecodeException cause = new EnvelopeDecodeException(
                "decode event envelope from message " + MESSAGE_ID + ": Unrecognized token 'not-json'");

        JsonNode json = mapper.readTree(mapper.writeValueAsString(publisher.envelope(message(), null, cause)));

        assertThat(json.get("correlation_id").asText()).isEmpty();
        JsonNode payload = json.get("payload");
        assertThat(payload.get("original_event_id").asText()).isEqualTo(MESSAGE_ID);
        assertThat(payload.get("original_event_type").asText()).isEqualTo("decode_failed");
        assertThat(payload.has("ride_id")).as("omitempty in the Go struct").isFalse();
        assertThat(payload.get("error").asText()).isEqualTo(cause.getMessage());
        assertThat(fieldNames(payload)).containsExactly("original_event_id", "original_event_type", "error", "service", "failed_at");
    }

    @Test
    void aFailureWithoutAMessageIsDescribedByItsClass() throws Exception {
        JsonNode json = mapper.readTree(mapper.writeValueAsString(
                publisher.envelope(message(), null, new NullPointerException())));

        assertThat(json.get("payload").get("error").asText()).isEqualTo("java.lang.NullPointerException");
    }

    @Test
    @SuppressWarnings("unchecked")
    void publishesOneEventFieldToTheDeadLetterStreamAndReportsSuccess() {
        RedisCommands<String, String> commands = mock(RedisCommands.class);
        when(commands.xadd(eq("events.dead_letter"), any(Map.class))).thenReturn("1788642800000-0");

        boolean published = publisher.publish(commands, message(), null, new EnvelopeDecodeException("bad"));

        assertThat(published).isTrue();
        ArgumentCaptor<Map<String, String>> body = ArgumentCaptor.forClass(Map.class);
        verify(commands).xadd(eq("events.dead_letter"), body.capture());
        assertThat(body.getValue()).containsOnlyKeys("event");
        assertThat(body.getValue().get("event")).startsWith("{\"id\":\"").contains("\"type\":\"dead_lettered\"");
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportsFailureInsteadOfThrowingWhenRedisDoesNotConfirm() {
        RedisCommands<String, String> commands = mock(RedisCommands.class);
        when(commands.xadd(eq("events.dead_letter"), any(Map.class)))
                .thenThrow(new RedisCommandTimeoutException("Command timed out after 2 second(s)"));

        assertThat(publisher.publish(commands, message(), null, new EnvelopeDecodeException("bad"))).isFalse();
    }

    private static StreamMessage<String, String> message() {
        return new StreamMessage<>(STREAM, MESSAGE_ID, Map.of(EnvelopeCodec.EVENT_FIELD, "not-json"));
    }

    private static List<String> fieldNames(JsonNode object) {
        List<String> names = new ArrayList<>();
        object.fieldNames().forEachRemaining(names::add);
        return names;
    }
}
