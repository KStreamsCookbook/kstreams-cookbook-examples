package org.kstreamscookbook.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.function.Supplier;

class CopyTopology implements Supplier<Topology> {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    @Override
    public Topology get() {
        var stringSerde = Serdes.String();

        var builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde))
                .to(OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

}
