package com.metroride.fare.pricing;

/**
 * A {@code ride_assigned} envelope could not be turned into a quote: its payload did not decode,
 * or its figures were rejected by the {@link FareCalculator}. Thrown inside the recording
 * transaction, so the event is not recorded either and the stream entry stays pending.
 */
public class FareQuoteException extends RuntimeException {

    public enum Reason {
        /** The payload was missing or not a {@code RideAssigned}. */
        PAYLOAD("payload"),
        /** The payload decoded but its figures were rejected. */
        CALCULATION("calculation");

        private final String label;

        Reason(String label) {
            this.label = label;
        }

        /** Value of the {@code reason} metric label. */
        public String label() {
            return label;
        }
    }

    private final Reason reason;

    public FareQuoteException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}
