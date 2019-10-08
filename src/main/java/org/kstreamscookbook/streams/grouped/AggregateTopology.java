package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.kstreamscookbook.TopologyBuilder;

/**
 * Joins message values into a CSV string.
 */
public class AggregateTopology implements TopologyBuilder {

    private final String sourceTopic;
    private final String targetTopic;

    public AggregateTopology(String sourceTopic, String targetTopic) {
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
    }

    @Override
    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();

        builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde))
                .groupByKey()
                .aggregate(() -> "",
                        (k, v, agg) -> (agg.length() == 0) ? v : agg + "," + v,
                        Materialized.as("csv-aggregation-store").with(stringSerde,stringSerde))
                .toStream()
                .to(targetTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

}
