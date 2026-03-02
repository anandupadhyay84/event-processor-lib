package com.here.dataprocessor.model;

import java.util.Objects;

/**
 * Immutable per-id aggregation result containing:
 * count, min timestamp, max timestamp, and mean value.
 */
public final class AggregationResult {

    private final long count;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final double mean;

    private AggregationResult(long count, long minTimestamp, long maxTimestamp, double mean) {
        this.count = count;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.mean = mean;
    }

    /**
     * Returns an empty aggregation result.
     */
    public static AggregationResult empty() {
        return new AggregationResult(0L, Long.MAX_VALUE, Long.MIN_VALUE, 0.0);
    }

    /**
     * Creates a result representing a single event.
     */
    public static AggregationResult ofSingle(long timestamp, double value) {
        return new AggregationResult(1L, timestamp, timestamp, value);
    }

    /**
     * Combines two aggregation results.
     * The operation is associative and safe for parallel reduction.
     */
    public static AggregationResult combine(AggregationResult left, AggregationResult right) {
        if (left.isEmpty()) return right;
        if (right.isEmpty()) return left;

        long combinedCount = left.count + right.count;
        long combinedMin = Math.min(left.minTimestamp, right.minTimestamp);
        long combinedMax = Math.max(left.maxTimestamp, right.maxTimestamp);

        // Weighted mean for numerical stability
        double combinedMean =
                (left.count * left.mean + right.count * right.mean) / combinedCount;

        return new AggregationResult(combinedCount, combinedMin, combinedMax, combinedMean);
    }

    /**
     * Returns a new result including the given event.
     * Uses incremental mean update for numerical stability.
     */
    public AggregationResult accumulate(long timestamp, double value) {
        if (isEmpty()) return ofSingle(timestamp, value);

        long newCount = count + 1;
        long newMin = Math.min(minTimestamp, timestamp);
        long newMax = Math.max(maxTimestamp, timestamp);

        double newMean = mean + (value - mean) / newCount;

        return new AggregationResult(newCount, newMin, newMax, newMean);
    }

    public long getCount() {
        return count;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public double getMean() {
        return mean;
    }

    /**
     * Returns true if no events were aggregated.
     */
    public boolean isEmpty() {
        return count == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggregationResult that)) return false;
        return count == that.count &&
               minTimestamp == that.minTimestamp &&
               maxTimestamp == that.maxTimestamp &&
               Double.compare(that.mean, mean) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, minTimestamp, maxTimestamp, mean);
    }

    @Override
    public String toString() {
        return String.format(
            "AggregationResult{count=%d, minTimestamp=%d, maxTimestamp=%d, mean=%.4f}",
            count, minTimestamp, maxTimestamp, mean
        );
    }
}