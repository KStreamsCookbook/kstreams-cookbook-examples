package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.function.Supplier;

public class BranchTopology implements Supplier<Topology> {
    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_ABC = "output-abc";
    public static final String OUTPUT_DEF = "output-def";
    public static final String OUTPUT_OTHER = "output-other";

    @Override
    public Topology get() {
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        @SuppressWarnings("unchecked")
        KStream<String, String>[] branches = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde))
                .branch((k, v) -> Arrays.asList('a', 'b', 'c').contains(v.charAt(0)),
                        (k, v) -> Arrays.asList('d', 'e', 'f').contains(v.charAt(0)),
                        (k, v) -> true);

        branches[0].to(OUTPUT_ABC, Produced.with(stringSerde, stringSerde));
        branches[1].to(OUTPUT_DEF, Produced.with(stringSerde, stringSerde));
        branches[2].to(OUTPUT_OTHER, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }
}
