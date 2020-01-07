package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.function.Supplier;

public class ReduceTopology implements Supplier<Topology> {

    private final String sourceTopic;
    private final String targetTopic;

    public ReduceTopology(String sourceTopic, String targetTopic) {
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
    }

    @Override
    public Topology get() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();

        builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde))
                .groupByKey()
                .reduce((agg, v) -> agg + "|" + v) //,
                        //Materialized.as("csv-aggregation-store").with(stringSerde,stringSerde))
                .toStream()
                .to(targetTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

}
