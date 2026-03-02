package com.here.dataprocessor.aggregator;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collector;
import java.util.stream.Stream;

import com.here.dataprocessor.model.AggregationResult;
import com.here.dataprocessor.model.Event;




/**
 * Thread-safe aggregator for computing per-id statistics
 * over a stream of events.
 *
 * Supports de-duplication by (id, timestamp) and
 * parallel stream execution.
 */
public final class EventAggregator {

	/**
	 * Private constructor to prevent instantiation. This is a utility class with
	 * static methods only.
	 */
	private EventAggregator() {
		throw new AssertionError("Utility class - do not instantiate");
	}

	/**
	 * Aggregates a stream of events into per-id statistics.
	 *
	 * Filters invalid events, removes duplicates based on (id, timestamp),
	 * and supports both sequential and parallel streams.
	 *
	 * Returns an immutable result map.
	 *
	 * @param events stream of events to aggregate
	 * @return unmodifiable map of id to AggregationResult
	 * @throws NullPointerException if events is null
	 */
	public static Map<String, AggregationResult> aggregate(Stream<Event> events) {
		Objects.requireNonNull(events, "Event stream cannot be null");

		Set<Event.DeduplicationKey> seenKeys = ConcurrentHashMap.newKeySet();

		Map<String, AggregationResult> result = events.filter(Objects::nonNull).filter(Event::isValid)
				.filter(e -> seenKeys.add(e.deduplicationKey())).collect(eventCollector());

		return Map.copyOf(result);
	}



	/**
	 * Creates a collector that aggregates events by id.
	 *
	 * The collector:
	 * - supports parallel aggregation (CONCURRENT)
	 * - does not depend on encounter order (UNORDERED)
	 *
	 * @return collector for aggregating events into a map of id to AggregationResult
	 */
	private static Collector<Event, ?, Map<String, AggregationResult>> eventCollector() {

		return Collector.of(ConcurrentHashMap::new, EventAggregator::accumulate, EventAggregator::combineMaps,
				Collector.Characteristics.UNORDERED, Collector.Characteristics.CONCURRENT);
	}

	/**
	 * Accumulates a single event into the aggregation map.
	 * Uses ConcurrentHashMap.merge for atomic, thread-safe updates.
	 * Creates a new AggregationResult if absent, otherwise combines.
	 */
	private static void accumulate(Map<String, AggregationResult> map, Event event) {

	    map.merge(
	        event.id(),
	        AggregationResult.ofSingle(event.timestamp(), event.value()),
	        AggregationResult::combine
	    );
	}

	/**
	 * Merges two partial aggregation maps during parallel execution.
	 * Combines results per id using associative AggregationResult.combine
	 * Returns the merged (left) map.
	 */
	private static Map<String, AggregationResult> combineMaps(
	        Map<String, AggregationResult> left,
	        Map<String, AggregationResult> right) {

	    right.forEach((id, stats) ->
	        left.merge(id, stats, AggregationResult::combine)
	    );

	    return left;
	}

}
