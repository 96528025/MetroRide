package com.metroride.fare.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.FareSettled;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

/** The JSON names of {@code FareSettled}, checked one by one against the tags in {@code events.go}. */
class FareSettledContractTest {

    /** The mapper Spring Boot builds (JSR-310 registered, timestamps as strings), as the service uses. */
    private final ObjectMapper mapper = Jackson2ObjectMapperBuilder.json().build();

    @Test
    void fieldNamesMatchTheGoStruct() {
        FareSettled settled = new FareSettled("ride-1", "rider-1", "driver-1", "assignment-1", "event-1",
                "5.85", "4.68", "1.17", "0.80", "2026-09-12T12:00:00.123456789Z");

        JsonNode json = mapper.valueToTree(settled);

        List<String> goTags = List.of("ride_id", "rider_id", "driver_id", "assignment_id", "settlement_event_id",
                "quote", "driver_amount", "platform_amount", "driver_share", "settled_at");
        assertThat(json.size()).isEqualTo(goTags.size());
        for (String tag : goTags) {
            assertThat(json.has(tag)).as(tag).isTrue();
            assertThat(json.get(tag).isTextual()).as(tag + " is a string").isTrue();
        }
        assertThat(json.get("quote").asText()).isEqualTo("5.85");
        assertThat(json.get("settlement_event_id").asText()).isEqualTo("event-1");
    }

    @Test
    void envelopeOccurredAtIsAStringWhateverTheMapperDefaults() throws Exception {
        Envelope envelope = new Envelope("e1", Envelope.TYPE_FARE_SETTLED, "fare-service", "ride-1",
                Instant.parse("2026-09-12T12:00:00.5Z"), mapper.createObjectNode());

        JsonNode json = mapper.readTree(mapper.writeValueAsString(envelope));

        assertThat(json.get("occurred_at").isTextual()).isTrue();
        assertThat(json.get("occurred_at").asText()).isEqualTo("2026-09-12T12:00:00.500Z");
        assertThat(json.get("type").asText()).isEqualTo("fare_settled");
        assertThat(Envelope.STREAM_RIDE_FARES).isEqualTo("events.ride.fares");
    }
}
