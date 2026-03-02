package com.here.dataprocessor.aggregator;

import com.here.dataprocessor.model.AggregationResult;
import com.here.dataprocessor.model.Event;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive test suite for {@link EventAggregator}.
 */
@DisplayName("EventAggregator Tests")
class EventAggregatorTest {

    // ==================== Basic Functionality Tests ====================

    @Test
    @DisplayName("Should return empty map for empty stream")
    void shouldReturnEmptyMapForEmptyStream() {
        Map<String, AggregationResult> result =
                EventAggregator.aggregate(Stream.empty());

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Should aggregate single event correctly")
    void shouldAggregateSingleEventCorrectly() {

        Event event = new Event("sensor-1", 1000L, 42.0);

        Map<String, AggregationResult> result =
                EventAggregator.aggregate(Stream.of(event));

        assertThat(result).hasSize(1);

        AggregationResult stats = result.get("sensor-1");
        assertThat(stats.getCount()).isEqualTo(1);
        assertThat(stats.getMinTimestamp()).isEqualTo(1000L);
        assertThat(stats.getMaxTimestamp()).isEqualTo(1000L);
        assertThat(stats.getMean()).isEqualTo(42.0);
    }

    @Test
    @DisplayName("Should aggregate multiple events for same id")
    void shouldAggregateMultipleEventsForSameId() {

        Stream<Event> events = Stream.of(
                new Event("sensor-1", 1000L, 10.0),
                new Event("sensor-1", 2000L, 20.0),
                new Event("sensor-1", 3000L, 30.0)
        );

        Map<String, AggregationResult> result =
                EventAggregator.aggregate(events);

        AggregationResult stats = result.get("sensor-1");

        assertThat(stats.getCount()).isEqualTo(3);
        assertThat(stats.getMinTimestamp()).isEqualTo(1000L);
        assertThat(stats.getMaxTimestamp()).isEqualTo(3000L);
        assertThat(stats.getMean()).isEqualTo(20.0);
    }

    @Test
    @DisplayName("Should aggregate events for multiple ids")
    void shouldAggregateEventsForMultipleIds() {

        Stream<Event> events = Stream.of(
                new Event("sensor-1", 1000L, 10.0),
                new Event("sensor-2", 1000L, 100.0),
                new Event("sensor-1", 2000L, 20.0),
                new Event("sensor-2", 2000L, 200.0)
        );

        Map<String, AggregationResult> result =
                EventAggregator.aggregate(events);

        assertThat(result).hasSize(2);

        assertThat(result.get("sensor-1").getMean()).isEqualTo(15.0);
        assertThat(result.get("sensor-2").getMean()).isEqualTo(150.0);
    }

    // ==================== Data Cleaning Tests ====================

    @Test
    @DisplayName("Should filter out events with NaN values")
    void shouldFilterOutEventsWithNaNValues() {

        Stream<Event> events = Stream.of(
                new Event("sensor-1", 1000L, 10.0),
                new Event("sensor-1", 2000L, Double.NaN),
                new Event("sensor-1", 3000L, 30.0)
        );

        Map<String, AggregationResult> result =
                EventAggregator.aggregate(events);

        AggregationResult stats = result.get("sensor-1");
        assertThat(stats.getCount()).isEqualTo(2);
        assertThat(stats.getMean()).isEqualTo(20.0);
    }

    @Test
    @DisplayName("Should filter out negative values")
    void shouldFilterOutNegativeValues() {

        Stream<Event> events = Stream.of(
                new Event("sensor-1", 1000L, 10.0),
                new Event("sensor-1", 2000L, -5.0),
                new Event("sensor-1", 3000L, 30.0)
        );

        Map<String, AggregationResult> result =
                EventAggregator.aggregate(events);

        assertThat(result.get("sensor-1").getCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("Should keep zero values")
    void shouldKeepZeroValues() {

        Stream<Event> events = Stream.of(
                new Event("sensor-1", 1000L, 0.0),
                new Event("sensor-1", 2000L, 10.0),
                new Event("sensor-1", 3000L, 20.0)
        );

        Map<String, AggregationResult> result =
                EventAggregator.aggregate(events);

        assertThat(result.get("sensor-1").getMean()).isEqualTo(10.0);
    }

    // ==================== Deduplication Tests ====================

    @Test
    @DisplayName("Should deduplicate events with same id and timestamp")
    void shouldDeduplicateEventsWithSameIdAndTimestamp() {

        Stream<Event> events = Stream.of(
                new Event("sensor-1", 1000L, 10.0),
                new Event("sensor-1", 1000L, 20.0),
                new Event("sensor-1", 2000L, 30.0)
        );

        Map<String, AggregationResult> result =
                EventAggregator.aggregate(events);

        AggregationResult stats = result.get("sensor-1");

        assertThat(stats.getCount()).isEqualTo(2);
        assertThat(stats.getMean()).isEqualTo(20.0);
    }

    @Test
    @DisplayName("Should handle multiple duplicates correctly")
    void shouldHandleMultipleDuplicatesCorrectly() {

        Stream<Event> events = Stream.of(
                new Event("sensor-1", 1000L, 10.0),
                new Event("sensor-1", 1000L, 99.0),
                new Event("sensor-1", 2000L, 20.0),
                new Event("sensor-2", 1000L, 100.0),
                new Event("sensor-2", 1000L, 99.0)
        );

        Map<String, AggregationResult> result =
                EventAggregator.aggregate(events);

        assertThat(result.get("sensor-1").getCount()).isEqualTo(2);
        assertThat(result.get("sensor-2").getCount()).isEqualTo(1);
    }

    // ==================== Parallel Tests ====================

    @Test
    @DisplayName("Sequential and parallel results should match")
    void shouldProduceSameResultsInParallel() {

        List<Event> events = IntStream.range(0, 10000)
                .mapToObj(i ->
                        new Event("sensor-" + (i % 10), i * 1000L, (double) i))
                .collect(Collectors.toList());

        Map<String, AggregationResult> sequential =
                EventAggregator.aggregate(events.stream());

        Map<String, AggregationResult> parallel =
                EventAggregator.aggregate(events.parallelStream());

        assertThat(parallel).hasSameSizeAs(sequential);

        for (String key : sequential.keySet()) {

            AggregationResult expected = sequential.get(key);
            AggregationResult actual = parallel.get(key);

            assertThat(actual.getCount()).isEqualTo(expected.getCount());
            assertThat(actual.getMinTimestamp()).isEqualTo(expected.getMinTimestamp());
            assertThat(actual.getMaxTimestamp()).isEqualTo(expected.getMaxTimestamp());

            // Use tolerance for floating-point comparison
            assertThat(actual.getMean())
                    .isCloseTo(expected.getMean(), within(1e-9));
        }
    }

    @Test
    @DisplayName("Should handle high concurrency with duplicates")
    void shouldHandleHighConcurrencyWithDuplicates() {

        List<Event> events = IntStream.range(0, 100000)
                .mapToObj(i ->
                        new Event("sensor-1", (i % 100) * 1000L, (double) i))
                .collect(Collectors.toList());

        Map<String, AggregationResult> result =
                EventAggregator.aggregate(events.parallelStream());

        assertThat(result.get("sensor-1").getCount()).isEqualTo(100);
    }

    // ==================== Null Safety ====================

    @Test
    @DisplayName("Should throw NPE for null stream")
    void shouldThrowForNullStream() {
        assertThatThrownBy(() -> EventAggregator.aggregate(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Event stream cannot be null");
    }

    @Test
    @DisplayName("Should skip null events in stream")
    void shouldSkipNullEventsInStream() {

        Stream<Event> events = Stream.of(
                new Event("sensor-1", 1000L, 10.0),
                null,
                new Event("sensor-1", 2000L, 20.0)
        );

        Map<String, AggregationResult> result =
                EventAggregator.aggregate(events);

        assertThat(result.get("sensor-1").getCount()).isEqualTo(2);
    }

    // ==================== AggregationResult Tests ====================

    @Test
    @DisplayName("AggregationResult combine should work correctly")
    void aggregationResultCombineShouldWork() {

        AggregationResult s1 = AggregationResult.ofSingle(1000L, 10.0);
        AggregationResult s2 = AggregationResult.ofSingle(2000L, 20.0);

        AggregationResult combined =
                AggregationResult.combine(s1, s2);

        assertThat(combined.getCount()).isEqualTo(2);
        assertThat(combined.getMean()).isEqualTo(15.0);
    }

    @Test
    @DisplayName("AggregationResult accumulate should work correctly")
    void aggregationResultAccumulateShouldWork() {

        AggregationResult stats = AggregationResult.empty()
                .accumulate(1000L, 10.0)
                .accumulate(2000L, 20.0)
                .accumulate(3000L, 30.0);

        assertThat(stats.getCount()).isEqualTo(3);
        assertThat(stats.getMean()).isEqualTo(20.0);
    }
}