package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class CountByValueLengthTopology implements Supplier<Topology> {

    private final String sourceTopic;
    private final String targetTopic;

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public CountByValueLengthTopology(String sourceTopic, String targetTopic) {
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
    }

    @Override
    public Topology get() {
        StreamsBuilder builder = new StreamsBuilder();

        Serde<String> stringSerde = Serdes.String();
        Serde<Integer> integerSerde = Serdes.Integer();
        Serde<Long> longSerde = Serdes.Long();


        builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde))
                .peek((k, v) -> log.info("Received ({}, {})", k, v))
                .groupBy((k, v) -> v.length(), Grouped.with(integerSerde, stringSerde))
                .count()
                .toStream()
                .peek((k, v) -> log.info("Sending ({}, {})", k, v))
                .to(targetTopic, Produced.with(integerSerde, longSerde));

        return builder.build();
    }

}
