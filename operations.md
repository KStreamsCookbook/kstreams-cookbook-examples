# Basics

* Basic structure of a Streams app
* Testing
  * https://kafka.apache.org/11/documentation/streams/developer-guide/testing.html


## KStreams

* `map`
  * Perform a 1:1 transformation of a (k,v) pair
* `mapValues`
  * Perform a 1:1 transformation of a value

* `filter`
  * Keep only things that match a predicate
* `filterNot`
  * Keep only things that do not match a predicate

* `flatMap`
  * Given a k,v pair, map to an Iterable of (k,v), each of which will be processed as its own message. Equivalent of Splitter.
* `flatMapValues`
  * Given a value, map to an Iterable of values, each of which will be processed as its own message. Equivalent of Splitter.

* `forEach(ForEachAction<K, V>)`
  * Perform an action on every value that goes through a pipeline, without emitting anything further.

* `peek`
  * Perform a stateless, record-by-record operation that triggers a side effect. Emits the k,v pair further.

## KGroupedStream - grouping functions

* Both of the following yield a `KGroupedStream`.
`Grouping is a prerequisite for aggregating a stream or a table and ensures that data is properly partitioned (“keyed”) for subsequent operations.`
  * `groupByKey` - Group the records by their current key
  * `groupBy` - Group the records of this KStream on a new key.
    * _When to set explicit SerDes: Variants of groupBy exist to override the configured default SerDes of your application, which you must do if the key and/or value types of the resulting KGroupedStream or KGroupedTable do not match the configured default SerDes._
    * Always causes data re-partitioning

* `count` - counts the number of records in this stream grouped by the same key
  * `CountByKeyTopology` - demonstrates counting using `groupByKey`
  * `CountByValueLengthTopology` - demonstrates counting using `groupBy`

* `aggregate`
  * `AggregateTopology`
  * `AggregateByValueLengthTopology` - demonstrates counting using `groupBy`

* `reduce`
  * Combines the values of records by key - the return type cannot be changed, unlike `aggregate` 
  * Rules:
    * Input records with null keys are ignored in general. 
    * When a record key is received for the first time, then the value of that record is used as the initial aggregate value for the next invocation.
The record itself skips the `reduce` step.
    * Whenever a record with a non-null value is received, the adder is called.

* `branch`

* `windowedBy`
  * windowed versus rolling aggregations

## KTables

From `count()`:

    Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to the same key. 
    The rate of propagated updates depends on your input data rate, the number of distinct keys, the number of parallel 
    running Kafka Streams instances, and the configuration parameters for cache size, and commit interval.
    
* `toStream` - convert back to a stream
  * `Consumed.with()`
  * `Materialized.as()` - allows you to name a state store, for querying outside the application 

TODO: naming intermediate topics 

----
* `join(KTable)`
* `join(KStream)`
* `leftJoin(KTable)`
* `leftJoin(KStream)`
* `merge(KStream)`
* `outerJoin(KTable)`
* `outerJoin(KStream)`

* `selectKey`
* `through`

* `process`
* `print`

* `transform`
* `transformValues`

* `windowedBy`
  * `grace` - how long to wait for late records
  * https://cwiki.apache.org/confluence/display/KAFKA/KIP-328%3A+Ability+to+suppress+updates+for+KTables#KIP-328:AbilitytosuppressupdatesforKTables-Suppressionpatterns

///
KTable<Windowed<String>, CountAndSum> hourlyResults =
    builder.stream("temperature-readings", Consumed.with(Serdes.String(), Serdes.Double()))
        .groupByKey() 
        .windowedBy(TimeWindows.of(Duration.ofHours(1)).grace(Duration.ofMinutes(10)))
        .aggregate(
            () -> new CountAndSum(0, 0),
            (k, v, agg) -> new CountAndSum(agg.count + 1, agg.sum + value))
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()));

// for the hr of 9:00-10:00, one event will be send at 10:10 (grace)

// one reading per sensor per hour
hourlyResults.mapValues(countAndSum, countAndSum.count)
    .toStream()
    .to("hourly-num-temperature-readings");
    
hourlyResults.mavValues(countAndSum -> countAndSum.sum / countAndSum.count)
    .toStream()
    .to("hourly-temperature-averages");perf

///

* suppress - spit out only one result per window KStreams 2.1+


Techniques
* forking
* windowing 
* splitting via processor
* punctuation
* error handling
* dead-letter channel

* Serdes:
  * Built-in
  * Avro serializer
  * JSON serializer
  * Protobuf serializer
  * custom serializer

Under the covers
* time
* state stores

