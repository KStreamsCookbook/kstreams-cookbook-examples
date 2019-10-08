package org.kstreamscookbook.tables;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.kstreamscookbook.TopologyBuilder;

/**
 * Emits all changes to a table as events on an output stream
 */
class SimpleTableTopology implements TopologyBuilder {

    private final String sourceTopic;
    private final String targetTopic;

    public SimpleTableTopology(String sourceTopic, String targetTopic) {
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
    }

    @Override
    public Topology build() {
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        builder.table(sourceTopic, Consumed.with(stringSerde, stringSerde))
                .toStream()
                .to(targetTopic, Produced.with(stringSerde, stringSerde));

        return builder.build();
    }

}
