package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.function.Supplier;

class MapValuesTopology implements Supplier<Topology> {

    private String inputTopic;
    private String outputTopic;

    public MapValuesTopology(String inputTopic, String outputTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    @Override
    public Topology get() {
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                .mapValues((ValueMapper<String, String>) String::toUpperCase)
                .to(outputTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

}
