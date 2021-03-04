package org.kstreamscookbook.streams.windowed;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.function.Supplier;

/**
 * Joins message values into a CSV string, depending on the window defined.
 */
public class SlidingWindowedAggregateTopology implements Supplier<Topology> {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final String sourceTopic;
    private final String targetTopic;
    private final Instant startTime;
    private final Duration timeDifferenceMs;
    private final Duration gracePeriod;

    public SlidingWindowedAggregateTopology(String sourceTopic, String targetTopic, Instant startTime, Duration timeDifferenceMs, Duration gracePeriod) {
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
        this.startTime = startTime;
        this.timeDifferenceMs = timeDifferenceMs;
        this.gracePeriod = gracePeriod;
    }

    @Override
    public Topology get() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();

        DateTimeFormatter formatter =
                DateTimeFormatter.ofLocalizedDateTime( FormatStyle.SHORT )
                        .withLocale( Locale.UK )
                        .withZone( ZoneId.systemDefault() );

        Materialized.as("csv-aggregation-store");
        builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde))
                .groupByKey()
                // messages are grouped into 5 minute windows, starting at midnight
                // Window is maintained for 24 hours
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(timeDifferenceMs, gracePeriod))
                .aggregate(() -> "",
                        (k, v, agg) -> (agg.length() == 0) ? v : agg + "," + v,
                        Materialized.with(stringSerde, stringSerde))
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
