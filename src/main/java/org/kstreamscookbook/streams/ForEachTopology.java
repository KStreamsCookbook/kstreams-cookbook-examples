package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.List;
import java.util.function.Supplier;

public class ForEachTopology implements Supplier<Topology> {

    private String inputTopic;

    public ForEachTopology(String inputTopic) {
        this.inputTopic = inputTopic;
    }

    private List<String> outputList;

    public ForEachTopology withOutputList(List<String> outputList) {
        this.outputList = outputList;
        return this;
    }

    @Override
    public Topology get() {
        Serde<Integer> integerSerde = Serdes.Integer();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(integerSerde, integerSerde))
                .filter((k, v) -> v >= 1000)
                .foreach((k, v) -> outputList.add(k + ":" + v));

        return builder.build();
    }

}
