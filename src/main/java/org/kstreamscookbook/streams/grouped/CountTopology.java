package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.function.Supplier;

public class CountTopology implements Supplier<Topology> {

    private final String sourceTopic;
    private final String targetTopic;

    public CountTopology(String sourceTopic, String targetTopic) {
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
    }

    @Override
    public Topology get() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde))
                .groupByKey()
                .count()
                .toStream()
                .to(targetTopic, Produced.with(stringSerde, longSerde));

        return builder.build();
    }

}
