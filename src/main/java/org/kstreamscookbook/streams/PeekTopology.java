package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.kstreamscookbook.TopologyBuilder;

import java.util.List;

class PeekTopology implements TopologyBuilder {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    private List<String> outputList;

    public PeekTopology withOutputList(List<String> outputList) {
        this.outputList = outputList;
        return this;
    }

    @Override
    public Topology build() {
        Serde<Integer> integerSerde = Serdes.Integer();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(integerSerde, integerSerde))
                .filter((k, v) -> v >= 1000)
                .peek((k, v) -> outputList.add(k + ":" + v))
                .to(OUTPUT_TOPIC, Produced.with(integerSerde, integerSerde));

        return builder.build();
    }

}
