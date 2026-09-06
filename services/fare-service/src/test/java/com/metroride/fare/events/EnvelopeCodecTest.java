package com.metroride.fare.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

class EnvelopeCodecTest {

    /**
     * A real entry from {@code events.ride.assignments}, captured with
     * {@code docker compose exec redis redis-cli XREVRANGE events.ride.assignments + - COUNT 1} against the
     * Compose stack right after dispatch-service assigned a ride (2026-09-05). Field order, the
     * nanosecond {@code occurred_at}, and the number formatting are exactly what Go's
     * {@code encoding/json} emits for {@code events.Envelope} and {@code events.RideAssigned}.
     */
    static final String GO_XADD_MESSAGE_ID = "1788642754475-0";
    static final String GO_XADD_EVENT_FIELD = "{\"id\":\"e017f057-3732-40c6-bc63-ecdfc35d4469\",\"type\":\"ride_assigned\",\"source\":\"dispatch-service\","
            + "\"correlation_id\":\"e7261e34-e49b-4fa0-bc86-1b412238f8c0\",\"occurred_at\":\"2026-09-05T21:12:34.293710969Z\","
            + "\"payload\":{\"ride_id\":\"e7261e34-e49b-4fa0-bc86-1b412238f8c0\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-1001\","
            + "\"distance_km\":0.03,\"eta_seconds\":60,\"assignment_id\":\"84b1ef4d-153d-4c60-a36d-c4ff2b2c008e\"}}";

    private final EnvelopeCodec codec = new EnvelopeCodec(Jackson2ObjectMapperBuilder.json().build());

    @Test
    void decodesTheGoEnvelopeFieldForField() {
        Envelope envelope = codec.decode(GO_XADD_MESSAGE_ID, Map.of(EnvelopeCodec.EVENT_FIELD, GO_XADD_EVENT_FIELD));

        assertThat(envelope.id()).isEqualTo("e017f057-3732-40c6-bc63-ecdfc35d4469");
        assertThat(envelope.type()).isEqualTo(Envelope.TYPE_RIDE_ASSIGNED);
        assertThat(envelope.source()).isEqualTo("dispatch-service");
        assertThat(envelope.correlationId()).isEqualTo("e7261e34-e49b-4fa0-bc86-1b412238f8c0");
        assertThat(envelope.occurredAt()).isEqualTo(Instant.parse("2026-09-05T21:12:34.293710969Z"));
        assertThat(envelope.payload().isObject()).isTrue();
        assertThat(envelope.payload().get("driver_id").asText()).isEqualTo("driver-1001");
    }

    @Test
    void decodesTheRideAssignedPayload() {
        Envelope envelope = codec.decode(GO_XADD_MESSAGE_ID, Map.of(EnvelopeCodec.EVENT_FIELD, GO_XADD_EVENT_FIELD));

        RideAssigned payload = codec.decodePayload(envelope, RideAssigned.class);

        assertThat(payload).isEqualTo(new RideAssigned(
                "e7261e34-e49b-4fa0-bc86-1b412238f8c0",
                "rider-42",
                "driver-1001",
                0.03,
                60,
                "84b1ef4d-153d-4c60-a36d-c4ff2b2c008e"));
    }

    @Test
    void acceptsShorterTimestampsAndUnknownFields() {
        String json = "{\"id\":\"e1\",\"type\":\"ride_assigned\",\"source\":\"dispatch-service\","
                + "\"correlation_id\":\"r1\",\"occurred_at\":\"2026-09-05T20:41:14.253817Z\","
                + "\"payload\":{},\"future_field\":true}";

        Envelope envelope = codec.decode("1-0", Map.of("event", json));

        assertThat(envelope.occurredAt()).isEqualTo(Instant.parse("2026-09-05T20:41:14.253817Z"));
    }

    @Test
    void rejectsMissingOrMismatchedPayloads() {
        Envelope noPayload = codec.decode("1-0", Map.of("event",
                "{\"id\":\"e1\",\"type\":\"ride_assigned\",\"payload\":null}"));
        assertThatThrownBy(() -> codec.decodePayload(noPayload, RideAssigned.class))
                .isInstanceOf(EnvelopeDecodeException.class)
                .hasMessageContaining("has no payload");

        Envelope wrongShape = codec.decode("1-0", Map.of("event",
                "{\"id\":\"e1\",\"type\":\"ride_assigned\",\"payload\":{\"distance_km\":\"far\"}}"));
        assertThatThrownBy(() -> codec.decodePayload(wrongShape, RideAssigned.class))
                .isInstanceOf(EnvelopeDecodeException.class)
                .hasMessageContaining("decode ride_assigned payload of event e1");
    }

    @Test
    void rejectsEntriesWithoutTheEventField() {
        assertThatThrownBy(() -> codec.decode("1-0", Map.of("other", "{}")))
                .isInstanceOf(EnvelopeDecodeException.class)
                .hasMessage("redis stream message 1-0 missing event field");
    }

    @Test
    void rejectsMalformedJsonAndMissingIdentity() {
        assertThatThrownBy(() -> codec.decode("1-0", Map.of("event", "{not json")))
                .isInstanceOf(EnvelopeDecodeException.class)
                .hasMessageContaining("decode event envelope");
        assertThatThrownBy(() -> codec.decode("1-0", Map.of("event", "{\"type\":\"ride_assigned\"}")))
                .isInstanceOf(EnvelopeDecodeException.class)
                .hasMessageContaining("has no id");
        assertThatThrownBy(() -> codec.decode("1-0", Map.of("event", "{\"id\":\"e1\"}")))
                .isInstanceOf(EnvelopeDecodeException.class)
                .hasMessageContaining("has no type");
    }
}
