package com.metroride.fare.processing;

import java.time.Instant;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {

    /**
     * Inserts the row unless an event with the same ID was already recorded. A single statement
     * with {@code ON CONFLICT DO NOTHING} is atomic under concurrent consumers; a select followed by
     * an insert would not be.
     *
     * @return {@code 1} when the row was inserted, {@code 0} when the event was already present
     */
    @Modifying
    @Query(value = """
            insert into fare.processed_events (event_id, stream, event_type, processed_at)
            values (:eventId, :stream, :eventType, :processedAt)
            on conflict (event_id) do nothing
            """, nativeQuery = true)
    int insertIfAbsent(
            @Param("eventId") String eventId,
            @Param("stream") String stream,
            @Param("eventType") String eventType,
            @Param("processedAt") Instant processedAt);
}
