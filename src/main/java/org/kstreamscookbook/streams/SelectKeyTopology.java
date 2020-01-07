package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.function.Supplier;

public class SelectKeyTopology implements Supplier<Topology> {
    private final String sourceTopic;
    private final String targetTopic;

    public SelectKeyTopology(String sourceTopic, String targetTopic) {
      this.sourceTopic = sourceTopic;
      this.targetTopic = targetTopic;
    }

    @Override
    public Topology get() {
      Serde<String> stringSerde = Serdes.String();

      StreamsBuilder builder = new StreamsBuilder();
      builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde))
              .selectKey((k, v) -> v.split(":")[0])
              .to(targetTopic, Produced.with(stringSerde, stringSerde));

      return builder.build();

    }
}
