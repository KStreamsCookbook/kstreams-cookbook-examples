package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.kstreamscookbook.TopologyBuilder;

import java.util.List;

class ForEachTopology implements TopologyBuilder {

    public static final String INPUT_TOPIC = "input-topic";

    private List<String> outputList;

    public ForEachTopology withOutputList(List<String> outputList) {
        this.outputList = outputList;
        return this;
    }

    @Override
    public Topology build() {
        Serde<Integer> integerSerde = Serdes.Integer();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(integerSerde, integerSerde))
                .filter((k, v) -> v >= 1000)
                .foreach((k, v) -> outputList.add(k + ":" + v));

        return builder.build();
    }

}
