package org.kstreamscookbook.streams.joins;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Supplier;

public class TimeWindowStreamInnerJoinStreamTopology implements Supplier<Topology> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final String leftSourceTopic;
    private final String rightSourceTopic;
    private final String targetTopic;

    public TimeWindowStreamInnerJoinStreamTopology(String leftSourceTopic, String rightSourceTopic, String targetTopic) {
        this.leftSourceTopic = leftSourceTopic;
        this.rightSourceTopic = rightSourceTopic;
        this.targetTopic = targetTopic;
    }

    @Override
    public Topology get() {
        var stringSerde = Serdes.String();
        var builder = new StreamsBuilder();

        final KStream<String, String> leftStream = builder.stream(leftSourceTopic, Consumed.with(stringSerde,stringSerde));
        final KStream<String, String> rightStream = builder.stream(rightSourceTopic, Consumed.with(stringSerde,stringSerde));

        leftStream.join(rightStream,
                (left,right) -> (right == null)? left : left + right,
                JoinWindows.of(Duration.ofSeconds(5)),
                StreamJoined.with(stringSerde, stringSerde, stringSerde))
            .peek((key, value) -> log.info("key = {} value = {}",key,value))
            .to(targetTopic,Produced.with(stringSerde,stringSerde)); // without the Produced.with(...) strange casting exceptions will appear

        return builder.build();
    }
}
