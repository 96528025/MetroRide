package com.metroride.fare.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Decodes Redis Stream entries the way {@code events.DecodeEnvelope} does on the Go side: the
 * entry has a single field named {@value #EVENT_FIELD} whose value is the JSON envelope written by
 * {@code events.Publish}. Anything else is a decode failure, not a message to skip silently.
 */
@Component
public class EnvelopeCodec {

    /** Stream entry field carrying the JSON envelope; see {@code events.Publish} in {@code events.go}. */
    public static final String EVENT_FIELD = "event";

    private final ObjectMapper mapper;

    public EnvelopeCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * @param messageId Redis stream entry ID, used only for error messages
     * @param fields    the entry's field/value pairs
     */
    public Envelope decode(String messageId, Map<String, String> fields) {
        String raw = fields.get(EVENT_FIELD);
        if (raw == null) {
            throw new EnvelopeDecodeException("redis stream message " + messageId + " missing event field");
        }
        Envelope envelope;
        try {
            envelope = mapper.readValue(raw, Envelope.class);
        } catch (JsonProcessingException e) {
            throw new EnvelopeDecodeException(
                    "decode event envelope from message " + messageId + ": " + e.getOriginalMessage(), e);
        }
        if (envelope == null || envelope.id() == null || envelope.id().isBlank()) {
            throw new EnvelopeDecodeException("event envelope in message " + messageId + " has no id");
        }
        if (envelope.type() == null || envelope.type().isBlank()) {
            throw new EnvelopeDecodeException("event envelope " + envelope.id() + " has no type");
        }
        return envelope;
    }

    /** Counterpart of {@code events.DecodePayload[T]}. */
    public <T> T decodePayload(Envelope envelope, Class<T> payloadType) {
        if (envelope.payload() == null || envelope.payload().isNull()) {
            throw new EnvelopeDecodeException("event envelope " + envelope.id() + " has no payload");
        }
        try {
            return mapper.treeToValue(envelope.payload(), payloadType);
        } catch (JsonProcessingException e) {
            throw new EnvelopeDecodeException(
                    "decode " + envelope.type() + " payload of event " + envelope.id() + ": " + e.getOriginalMessage(), e);
        }
    }
}
