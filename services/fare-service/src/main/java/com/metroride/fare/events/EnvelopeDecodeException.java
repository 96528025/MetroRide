package com.metroride.fare.events;

/** A stream entry could not be turned into an {@link Envelope}; the entry is not acknowledged. */
public class EnvelopeDecodeException extends RuntimeException {

    public EnvelopeDecodeException(String message) {
        super(message);
    }

    public EnvelopeDecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
