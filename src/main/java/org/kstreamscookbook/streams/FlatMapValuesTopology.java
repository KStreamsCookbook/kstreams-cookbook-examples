package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Supplier;

public class FlatMapValuesTopology implements Supplier<Topology> {

    private String inputTopic;
    private String outputTopic;

    public FlatMapValuesTopology(String inputTopic, String outputTopic) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    @Override
    public Topology get() {
        Logger log = LoggerFactory.getLogger(this.getClass());

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                // ValueMapper#(s):Iterable<?>
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .peek((key, value) -> log.info(value))
                .to(outputTopic, Produced.with(stringSerde, stringSerde));
        return builder.build();
    }
}
