package org.kstreamscookbook.streams.windowed;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Joins message values into a CSV string, depending on the window defined.
 */
public class SessionWindowedAggregateTopology implements Supplier<Topology> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final String sourceTopic;
    private final String targetTopic;
    private final Duration duration;

    public SessionWindowedAggregateTopology(String sourceTopic, String targetTopic, Duration duration) {
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
        this.duration = duration;
    }

    @Override
    public Topology get() {
        var builder = new StreamsBuilder();
        var stringSerde = Serdes.String();

        builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde))
                .groupByKey()
                // messages are grouped into 5 minute windows, starting at midnight
                // Window is maintained for 24 hours
                .windowedBy(SessionWindows.with(duration))
                .aggregate(() -> "",
                        new MyAggregator(),
                        new MyMerger(),
                        Materialized.as("csv-aggregation-store").with(stringSerde, stringSerde))
                .toStream()
                .filter((k,v) -> v != null) // remove tombstones added by the aggregate for internal reasons, see https://issues.apache.org/jira/browse/KAFKA-8318
                // the windowed key allows us access to metadata about the window, such as start and end times
                .peek((windowedKey, v) -> {
                    String key = windowedKey.key();
                    Window window = windowedKey.window();
                    // note that the start end end times of the window are known for all messages in a time window
                    log.info("Window start time: {}; Window end time: {}; key: {} -> value: {}",
                            window.startTime().toString(),
                            window.endTime().toString(),
                            key,
                            v);
                })
                // transform the windowed key back to a String for serialization
                 .map((key, value) -> new KeyValue<>(key.key(), value))
                .to(targetTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

    private class MyAggregator implements Aggregator<String, String, String> {

        @Override
        public String apply(String k, String v, String agg) {
            log.info("MyAggregator called with k={}, v={}, agg={}",k,v,agg);
            String result = (agg.length() == 0) ? v : agg + "," + v;
            return result;
        }
    }

    private class MyMerger implements Merger<String, String> {
        @Override
        public String apply(String aggKey, String aggOne, String aggTwo) {
            if (aggOne.length() == 0) {
                return aggTwo;
            }
            else {
                log.info("MyMerger called with key = {}, one={} two={}", aggKey, aggOne, aggTwo);
                String result = "[(" + aggOne + "),(" + aggTwo + ")]";
                return result;
            }
        }
    }
}
