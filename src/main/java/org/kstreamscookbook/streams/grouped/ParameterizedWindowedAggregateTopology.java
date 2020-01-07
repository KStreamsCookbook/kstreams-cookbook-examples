package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.kstreamscookbook.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

/**
 * Joins message values into a CSV string, depending on the window defined.
 */
public class ParameterizedWindowedAggregateTopology implements TopologyBuilder {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final String sourceTopic;
    private final String targetTopic;
    private final TimeWindows windows;
    private final Instant startTime;

    public ParameterizedWindowedAggregateTopology(String sourceTopic, String targetTopic, TimeWindows windows, Instant startTime) {
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
        this.windows = windows;
        this.startTime = startTime;
    }

    @Override
    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();

        DateTimeFormatter formatter =
                DateTimeFormatter.ofLocalizedDateTime( FormatStyle.SHORT )
                        .withLocale( Locale.UK )
                        .withZone( ZoneId.systemDefault() );

        builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde))
                .groupByKey()
                // messages are grouped into 5 minute windows, starting at midnight
                // Window is maintained for 24 hours
                .windowedBy(windows)
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
                 .map(mapByTimeWindow())
                .to(targetTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

    private KeyValueMapper<Windowed<String>, String, KeyValue<? extends String, ? extends String>> mapByTimeWindow() {
        return (key, value) ->
                new KeyValue<>(
                        key.key() + "@(" +
                                ChronoUnit.SECONDS.between(startTime,key.window().startTime()) +
                                "," +
                                ChronoUnit.SECONDS.between(startTime,key.window().endTime()) +
                                ")",
                        value);
    }

}
