package org.kstreamscookbook.app;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

class CopyTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopologySupplier() {
        return new CopyTopology();
    }

    @Test
    void testCopied() {
        StringSerializer stringSerializer = new StringSerializer();
        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

        // NOTE: you have to keep using the topic name when sending String keys to distinguish between
        // factory.create(K, V) and factory.create(topicName:String, V)
        // otherwise you can set the topic name when creating the ConsumerRecordFactory
        testDriver.pipeInput(factory.create(CopyTopology.INPUT_TOPIC, "key", "value"));

        StringDeserializer stringDeserializer = new StringDeserializer();
        ProducerRecord<String, String> producerRecord = testDriver.readOutput(CopyTopology.OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        OutputVerifier.compareKeyValue(producerRecord, "key", "value");
    }
}