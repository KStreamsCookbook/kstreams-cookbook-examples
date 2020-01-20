package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;
import org.kstreamscookbook.streams.CopyTopology;

import java.util.function.Supplier;

class CopyTopologyTest extends TopologyTestBase {

    private final static String INPUT_TOPIC = "input-topic";
    private final static String OUTPUT_TOPIC = "output-topic";

    @Override
    protected Supplier<Topology> withTopology() {
        return new CopyTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    void testCopied() {
        var stringSerializer = new StringSerializer();
        var factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

        // NOTE: you have to keep using the topic name when sending String keys to distinguish between
        // factory.create(K, V) and factory.create(topicName:String, V)
        // otherwise you can set the topic name when creating the ConsumerRecordFactory
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "key", "value"));

        var stringDeserializer = new StringDeserializer();
        var producerRecord = testDriver.readOutput(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        OutputVerifier.compareKeyValue(producerRecord, "key", "value");
    }
}