package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.kstreamscookbook.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms a stream of (keys : string values) into
 * a stream of (value lengths : CSV of the keys that sent values of those lengths)
 */
public class AggregateByValueLengthTopology implements TopologyBuilder {

    private final String sourceTopic;
    private final String targetTopic;

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public AggregateByValueLengthTopology(String sourceTopic, String targetTopic) {
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
    }

    @Override
    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();
        Serde<Integer> integerSerde = Serdes.Integer();

        builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde))
                .peek((k, v) -> log.info("Received ({}, {})", k, v))
                // swap the key and the value, so ("x", "...") -> ("...", "x")
                .map((k, v) -> new KeyValue<>(v, k))
                .groupBy((k, v) -> k.length(), Grouped.with(integerSerde, stringSerde))
                .aggregate(() -> "",
                        (k, v, agg) -> (agg.length() == 0) ? v : agg + "," + v,
                        Materialized.as("csv-aggregation-store").with(integerSerde, stringSerde))
                .toStream()
                .peek((k, v) -> log.info("Sending ({}, {})", k, v))
                .to(targetTopic, Produced.with(integerSerde, stringSerde));

        return builder.build();
    }

}
