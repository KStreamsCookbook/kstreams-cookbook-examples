package org.kstreamscookbook.tables;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

class SimpleTableTopologyTest extends TopologyTestBase {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    @Override
    protected Supplier<Topology> withTopologySupplier() {
        return new SimpleTableTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    void testCopied() {
        StringSerializer stringSerializer = new StringSerializer();
        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

        // NOTE: you have to keep using the topic name when sending String keys to distinguish between
        // factory.create(K, V) and factory.create(topicName:String, V)
        // otherwise you can set the topic name when creating the ConsumerRecordFactory
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "one"));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "b", "one"));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "two"));

        expectNextKVPair("a", "one");
        expectNextKVPair("b", "one");
        expectNextKVPair("a", "two");

    }

    private void expectNextKVPair(String k, String v) {
        StringDeserializer stringDeserializer = new StringDeserializer();
        ProducerRecord<String, String> producerRecord = testDriver.readOutput(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);
        OutputVerifier.compareKeyValue(producerRecord, k, v);
    }

}