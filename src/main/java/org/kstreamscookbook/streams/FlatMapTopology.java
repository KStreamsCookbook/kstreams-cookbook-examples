package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

class FlatMapTopology implements Supplier<Topology> {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    @Override
    public Topology get() {
        Logger log = LoggerFactory.getLogger(this.getClass());

        Serde<String> stringSerde = Serdes.String();
        var intSerde = Serdes.Integer();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde))
                // KeyValueMapper#(k,s):Iterable<KeyValue<k,v>>
                .flatMap((key, value) -> {
                    String[] words = value.split("\\W+");
                    List<KeyValue<Integer, String>> result = new LinkedList<>();
                    int index = 0;
                    for (String word : words) {
                        result.add(KeyValue.pair(index++, word));
                    }
                    return result;
                })
                // TODO this performs a re-keying
                .peek((key, value) -> log.info(value))
                .to(OUTPUT_TOPIC, Produced.with(intSerde, stringSerde));
        return builder.build();
    }
}
