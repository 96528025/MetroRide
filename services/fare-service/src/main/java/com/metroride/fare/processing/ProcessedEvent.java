package com.metroride.fare.processing;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * One row per event envelope this service has consumed. The primary key on {@code event_id} is
 * what makes redelivery safe: a second delivery of the same envelope inserts nothing.
 *
 * <p>The table is created by {@code db/migration/V1__processed_events.sql}; Hibernate only
 * validates this mapping against it ({@code spring.jpa.hibernate.ddl-auto=validate}). The
 * {@code columnDefinition} values repeat the migration's types so that validation compares like
 * with like.
 */
@Entity
@Table(name = "processed_events", schema = "fare")
public class ProcessedEvent {

    @Id
    @Column(name = "event_id", nullable = false, columnDefinition = "text")
    private String eventId;

    @Column(name = "stream", nullable = false, columnDefinition = "text")
    private String stream;

    @Column(name = "event_type", nullable = false, columnDefinition = "text")
    private String eventType;

    @Column(name = "processed_at", nullable = false, columnDefinition = "timestamptz")
    private Instant processedAt;

    protected ProcessedEvent() {
        // JPA
    }

    public String getEventId() {
        return eventId;
    }

    public String getStream() {
        return stream;
    }

    public String getEventType() {
        return eventType;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
