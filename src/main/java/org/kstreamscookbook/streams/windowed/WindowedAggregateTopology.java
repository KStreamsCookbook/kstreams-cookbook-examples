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
public class WindowedAggregateTopology implements Supplier<Topology> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final String sourceTopic;
    private final String targetTopic;

    public WindowedAggregateTopology(String sourceTopic, String targetTopic) {
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
    }

    @Override
    public Topology get() {
        var builder = new StreamsBuilder();
        var stringSerde = Serdes.String();

        builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde))
                .groupByKey()
                // messages are grouped into 5 minute windows, starting at midnight
                // Window is maintained for 24 hours
                .windowedBy(TimeWindows.of(Duration.ofMinutes(5)))
                .aggregate(() -> "",
                        (k, v, agg) -> (agg.length() == 0) ? v : agg + "," + v,
                        Materialized.as("csv-aggregation-store").with(stringSerde, stringSerde))
                .toStream()
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

}
