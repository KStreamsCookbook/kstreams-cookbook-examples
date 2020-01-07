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

class FlatMapValuesTopology implements Supplier<Topology> {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    @Override
    public Topology get() {
        Logger log = LoggerFactory.getLogger(this.getClass());

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde))
                // ValueMapper#(s):Iterable<?>
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .peek((key, value) -> log.info(value))
                .to(OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));
        return builder.build();
    }
}
