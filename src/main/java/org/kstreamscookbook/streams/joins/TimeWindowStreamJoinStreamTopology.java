package org.kstreamscookbook.streams.joins;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Interface to store a reference to the join method
 * @param <K> Key type
 * @param <VR> Value type
 * @param <VO> Other Value type
 * @param <V> result Value type
 */
interface Joiner<K,VR,VO,V> {
    KStream<K, VR> join(KStream<K, VO> kStream,
                        org.apache.kafka.streams.kstream.ValueJoiner<? super V, ? super VO, ? extends VR> valueJoiner,
                        org.apache.kafka.streams.kstream.JoinWindows joinWindows,
                        org.apache.kafka.streams.kstream.StreamJoined<K, V, VO> streamJoined);
}

/**
 * Choice of Join Method
 */
enum JoinMethod {
    INNER_JOIN,
    LEFT_JOIN,
    OUTER_JOIN,
}

public class TimeWindowStreamJoinStreamTopology implements Supplier<Topology> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final String leftSourceTopic;
    private final String rightSourceTopic;
    private final String targetTopic;
    private final JoinMethod joinMethod;

    public TimeWindowStreamJoinStreamTopology(JoinMethod joinMethod, String leftSourceTopic, String rightSourceTopic, String targetTopic) {
        this.joinMethod = joinMethod;
        this.leftSourceTopic = leftSourceTopic;
        this.rightSourceTopic = rightSourceTopic;
        this.targetTopic = targetTopic;
    }

    class MyJoiner implements ValueJoiner<String, String, String> {

        @Override
        public String apply(String left, String right) {
            if (right == null)
                return left;
            else if (left == null)
                return right;
            else
                return left + right;
        }
    }

    class Peeker implements ForeachAction<String, String> {

        @Override
        public void apply(String key, String value) {
            log.info("key = {} value = {}",key,value);
        }
    }

    @Override
    public Topology get() {
        var stringSerde = Serdes.String();
        var builder = new StreamsBuilder();

        final KStream<String, String> leftStream = builder.stream(leftSourceTopic, Consumed.with(stringSerde,stringSerde));
        final KStream<String, String> rightStream = builder.stream(rightSourceTopic, Consumed.with(stringSerde,stringSerde));

        Joiner<String,String,String,String> method = null;
        switch(joinMethod) {
            case INNER_JOIN:
                method = leftStream::join;
                break;
            case LEFT_JOIN:
                method = leftStream::leftJoin;
                break;
            case OUTER_JOIN:
                method = leftStream::outerJoin;
                break;
            default:
                log.error("Unknown JoinMethod -> exiting");
                System.exit(1);
        }

        method.join(rightStream,
                new MyJoiner(),
                JoinWindows.of(Duration.ofSeconds(5)),
                StreamJoined.with(stringSerde, stringSerde, stringSerde))
            .peek(new Peeker())
            .to(targetTopic,Produced.with(stringSerde,stringSerde)); // without the Produced.with(...) strange casting exceptions will appear

        return builder.build();
    }
}
