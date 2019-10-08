package org.kstreamscookbook.tables;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.kstreamscookbook.TopologyBuilder;

/**
 * Emits all changes to a table as events on an output stream
 */
class MaterializedTableTopology implements TopologyBuilder {

    private final String sourceTopic;
    private final String targetTopic;

    public MaterializedTableTopology(String sourceTopic, String targetTopic) {
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
    }

    @Override
    public Topology build() {
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        builder.table(sourceTopic,
                    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("my-table")
                        .withKeySerde(stringSerde)
                        .withValueSerde(stringSerde))
                .toStream()
                .to(targetTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

}
