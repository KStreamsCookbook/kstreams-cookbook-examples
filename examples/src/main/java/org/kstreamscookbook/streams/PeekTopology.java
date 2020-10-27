package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.List;
import java.util.function.Supplier;

class PeekTopology implements Supplier<Topology> {
    private String inputTopic;
    private String outputTopic;

    public PeekTopology(String inputTopic, String outputTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    private List<String> outputList;

    public PeekTopology withOutputList(List<String> outputList) {
        this.outputList = outputList;
        return this;
    }

    @Override
    public Topology get() {
        Serde<Integer> integerSerde = Serdes.Integer();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(integerSerde, integerSerde))
                .filter((k, v) -> v >= 1000)
                .peek((k, v) -> outputList.add(k + ":" + v))
                .to(outputTopic, Produced.with(integerSerde, integerSerde));

        return builder.build();
    }

}
