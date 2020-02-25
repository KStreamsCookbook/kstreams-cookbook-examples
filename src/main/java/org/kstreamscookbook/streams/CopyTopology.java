package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.function.Supplier;

public class CopyTopology implements Supplier<Topology> {
    private final String inputTopic;
    private final String outputTopic;

    public CopyTopology(String inputTopic, String outputTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    @Override
    public Topology get() {
        var stringSerde = Serdes.String();

        var builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

}
