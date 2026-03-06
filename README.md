# Event Processor Library

A Thread-safe and memory-efficient Java library for processing streams of domain events with aggregation capabilities.

---

## Overview

This library processes a finite but potentially large stream of events and computes aggregated statistics per unique event identifier.

It is designed to handle:

- Out-of-order events — chronological ordering is not required
- Duplicate events — automatic deduplication based on `(id, timestamp)`
- Invalid data — filters NaN and negative values
- Parallel execution — full support for parallel stream processing
- Memory efficiency — streaming processing without full materialization

---

## Features

- **Thread-safe** — Concurrent aggregation using `ConcurrentHashMap`
- **Parallel-ready** — Optimized for parallel stream execution
- **Memory-efficient** — O(k + d) space complexity
- **Numerically stable** — Accurate mean calculation using weighted mean for parallel aggregation
- **Well-tested** — Comprehensive test suite with edge case coverage
- **Modern Java** — Uses Java 17+ features (records, pattern matching, streams)

---

## Project Structure

```
event-processor-lib/
├── pom.xml                          # Maven build configuration
├── README.md                        # This file
└── src/
    ├── main/java/com/here/dataprocessor/
    │   ├── model/
    │   │   ├── Event.java               # Event record definition
    │   │   └── AggregationResult.java   # Immutable aggregation statistics
    │   └── aggregator/
    │       └── EventAggregator.java     # Main aggregation logic
    └── test/java/com/here/dataprocessor/
        └── aggregator/
            └── EventAggregatorTest.java # Comprehensive test suite
```

---

## Building and Running

### Prerequisites

- Java 17 or higher  
- Maven 3.8+

### Compile

```bash
mvn clean compile
```

### Run Tests

```bash
mvn test
```

### Package

```bash
mvn clean package
```

---

## Usage Example

```java
import com.here.dataprocessor.model.Event;
import com.here.dataprocessor.model.AggregationResult;
import com.here.dataprocessor.aggregator.EventAggregator;

import java.util.Map;
import java.util.stream.Stream;

Stream<Event> events = Stream.of(
    new Event("sensor-1", 1000L, 10.0),
    new Event("sensor-1", 2000L, 20.0),
    new Event("sensor-2", 1000L, 100.0)
);

Map<String, AggregationResult> result =
        EventAggregator.aggregate(events);

AggregationResult sensor1Stats = result.get("sensor-1");
```

---

## Parallel Processing

Parallel execution is supported but not mandated.

```java
Map<String, AggregationResult> result =
        EventAggregator.aggregate(events.parallel());
```

### Architecture Overview & Decisions

The library does not internally enforce parallel execution.  
Instead, it allows the caller to decide whether to process sequentially or in parallel.

This approach:

- Preserves architectural flexibility
- Avoids implicit thread usage
- Enables integration into larger processing pipelines
- Allows performance tuning at application level

---

## Design Decisions

### Concurrency Strategy

- Uses `ConcurrentHashMap` for thread-safe aggregation
- Collector marked as `CONCURRENT` and `UNORDERED`
- Atomic `merge()` operations for safe parallel accumulation
- `AggregationResult` is immutable

---

### Deduplication Strategy

- Duplicate definition: identical `(id, timestamp)`
- First occurrence wins
- Thread-safe deduplication using `ConcurrentHashMap.newKeySet()`
- O(1) expected lookup time per event

---

### Memory Trade-off

Exact deduplication requires storing unique `(id, timestamp)` keys.

Memory usage is proportional to:

```
O(k + d)
```

Where:

- `k` = number of unique ids  
- `d` = number of unique `(id, timestamp)` pairs  

This trade-off is unavoidable for exact deduplication.

Estimated impact at scale:

- **~10 million unique events** → Manageable with sufficient JVM heap (e.g., 2–4GB)
- **~100 million unique events** → High memory pressure
- **~1 billion unique events** → Likely to cause `OutOfMemoryError`

For unbounded streams or extremely high workloads, alternative approaches would be required:

- Time-bounded deduplication (sliding window)
- Probabilistic data structures (e.g., Bloom filters)
- Externalized state storage

This implementation assumes a finite stream as defined in the assignment.

---

### Numerical Stability

Mean calculation is performed using a weighted mean aggregation formula when combining partial results.

```
combinedMean = (left.count * left.mean + right.count * right.mean) / combinedCount
```

This approach avoids recomputing the mean from raw values and maintains numerical stability when handling large datasets.

---

## Complexity Analysis

- **Time Complexity:** O(n)
- **Space Complexity:** O(k + d)

Additional considerations:

- Deduplication lookup: O(1) average (hash-based)
- Parallel merge cost: O(p)

Where:

- n = total number of events
- k = number of unique ids
- d = number of unique (id, timestamp) pairs
- p = number of parallel threads

---

## Assumptions

1. Stream is finite but potentially large (Current Solution can handle upt 10M records easily and upto 100M records with increase in heap size).
2. Timestamp is epoch milliseconds  
3. Duplicate = same id + timestamp  
4. First occurrence is retained  
5. NaN and negative values are considered invalid  

---

## Testing

The test suite validates:

- Empty stream handling
- Multiple identifiers
- Deduplication correctness
- Parallel execution consistency
- Floating-point precision tolerance
- High concurrency scenarios
- Null safety

Run tests with:

```bash
mvn test
```

---

## Final Notes

This implementation prioritizes:

- Correctness  
- Thread safety  
- Streaming design  
- Architectural flexibility  
- Explicit memory trade-off awareness  
