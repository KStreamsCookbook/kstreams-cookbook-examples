package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.kstreamscookbook.TopologyBuilder;

import java.util.Arrays;

public class BranchEnumTopology implements TopologyBuilder {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_ABC = "output-abc";
    public static final String OUTPUT_DEF = "output-def";
    public static final String OUTPUT_OTHER = "output-other";

    enum Characters {
        a_to_c((k, v) -> Arrays.asList('a', 'b', 'c').contains(v.charAt(0))),
        d_to_f((k, v) -> Arrays.asList('d', 'e', 'f').contains(v.charAt(0))),
        others((k, v) -> true);

        public Predicate<String, String> matches;

        Characters(Predicate<String, String> matches) {
            this.matches = matches;
        }
    }

    @Override
    public Topology build() {
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        @SuppressWarnings("unchecked")
        KStream<String, String>[] branches = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde))
                .branch(Characters.a_to_c.matches,
                        Characters.d_to_f.matches,
                        Characters.others.matches);

        // refer to the branches via the prosition of the predicate in the enum
        branches[Characters.a_to_c.ordinal()].to(OUTPUT_ABC, Produced.with(stringSerde, stringSerde));

        branches[Characters.d_to_f.ordinal()].to(OUTPUT_DEF, Produced.with(stringSerde, stringSerde));

        branches[Characters.others.ordinal()].to(OUTPUT_OTHER, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }
}
