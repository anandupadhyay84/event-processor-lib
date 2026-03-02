package com.here.dataprocessor.model;

import java.util.Objects;

/**
 * Immutable domain event containing an id, time-stamp (epoch millis),
 * and associated numeric value.
 */
public record Event(String id, long timestamp, double value) {

    /**
     * Validates that id is non-null.
     */
    public Event {
        Objects.requireNonNull(id, "Event id cannot be null");
    }

    
    /**
     * Returns true if the event value is non-negative and not NaN.
     */
    public boolean isValid() {
        return !Double.isNaN(value) && value >= 0;
    }

    
    /**
     * Returns a composite key based on (id, time-stamp) used for de-duplication.
     */
    public DeduplicationKey deduplicationKey() {
        return new DeduplicationKey(id, timestamp);
    }
    

    /**
     * Composite key identifying duplicate events
     * by (id, time-stamp).
     */
    public record DeduplicationKey(String id, long timestamp) {}
}

