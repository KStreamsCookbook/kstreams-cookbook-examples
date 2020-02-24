package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.function.Supplier;

public class FilterTopology implements Supplier<Topology> {

    private String inputTopic;
    private String outputTopic;

    public FilterTopology(String inputTopic, String outputTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    @Override
    public Topology get() {
        Serde<Integer> integerSerde = Serdes.Integer();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(integerSerde, integerSerde))
                .filter((k, v) -> v >= 1000)
                .to(outputTopic, Produced.with(integerSerde, integerSerde));

        return builder.build();
    }

}
