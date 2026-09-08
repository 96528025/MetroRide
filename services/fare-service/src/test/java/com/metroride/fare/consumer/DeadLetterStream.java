package com.metroride.fare.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Test-side reader of {@code events.dead_letter}: the stream is shared, so entries are found by content. */
final class DeadLetterStream {

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;

    DeadLetterStream(StringRedisTemplate redis, ObjectMapper mapper) {
        this.redis = redis;
        this.mapper = mapper;
    }

    long length() {
        Long size = redis.opsForStream().size(Envelope.STREAM_DEAD_LETTER);
        return size == null ? 0 : size;
    }

    /** The dead-letter envelope whose payload names {@code originalEventId}, decoded as JSON. */
    Optional<JsonNode> find(String originalEventId) {
        List<MapRecord<String, Object, Object>> records =
                redis.opsForStream().range(Envelope.STREAM_DEAD_LETTER, Range.unbounded());
        if (records == null) {
            return Optional.empty();
        }
        return records.stream()
                .map(this::envelope)
                .filter(json -> originalEventId.equals(json.path("payload").path("original_event_id").asText()))
                .findFirst();
    }

    long count(String originalEventId) {
        List<MapRecord<String, Object, Object>> records =
                redis.opsForStream().range(Envelope.STREAM_DEAD_LETTER, Range.unbounded());
        return records == null ? 0 : records.stream()
                .map(this::envelope)
                .filter(json -> originalEventId.equals(json.path("payload").path("original_event_id").asText()))
                .count();
    }

    private JsonNode envelope(MapRecord<String, Object, Object> record) {
        try {
            return mapper.readTree(String.valueOf(record.getValue().get(EnvelopeCodec.EVENT_FIELD)));
        } catch (java.io.IOException e) {
            throw new IllegalStateException("dead letter " + record.getId() + " is not JSON", e);
        }
    }
}
