package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.function.Supplier;

class FilterNotTopology implements Supplier<Topology> {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    @Override
    public Topology get() {
        Serde<Integer> integerSerde = Serdes.Integer();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(integerSerde, stringSerde))
                // process only accounts not in the UK
                .filterNot((k, v) -> v.equals("UK"))
                .to(OUTPUT_TOPIC, Produced.with(integerSerde, stringSerde));

        return builder.build();
    }

}
