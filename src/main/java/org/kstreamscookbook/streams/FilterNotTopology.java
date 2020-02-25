package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.function.Supplier;

public class FilterNotTopology implements Supplier<Topology> {
    private String inputTopic;
    private String outputTopic;

    public FilterNotTopology(String inputTopic, String outputTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    @Override
    public Topology get() {
        Serde<Integer> integerSerde = Serdes.Integer();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(integerSerde, stringSerde))
                // process only accounts not in the UK
                .filterNot((k, v) -> v.equals("UK"))
                .to(outputTopic, Produced.with(integerSerde, stringSerde));

        return builder.build();
    }

}
