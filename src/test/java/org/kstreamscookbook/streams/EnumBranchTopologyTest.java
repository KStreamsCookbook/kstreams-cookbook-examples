package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

class EnumBranchTopologyTest {

    private TopologyTestDriver testDriver;

    @BeforeEach
    void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(new EnumBranchTopology().build(), config);
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void testCopied() {
        StringSerializer stringSerializer = new StringSerializer();
        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

        testDriver.pipeInput(factory.create(EnumBranchTopology.INPUT_TOPIC, "one", "alpha"));
        testDriver.pipeInput(factory.create(EnumBranchTopology.INPUT_TOPIC, "two", "delta"));
        testDriver.pipeInput(factory.create(EnumBranchTopology.INPUT_TOPIC, "three", "tango"));

        StringDeserializer stringDeserializer = new StringDeserializer();

        {
            ProducerRecord<String, String> producerRecord = testDriver.readOutput(EnumBranchTopology.OUTPUT_ABC, stringDeserializer, stringDeserializer);
            OutputVerifier.compareKeyValue(producerRecord, "one", "alpha");
        }
        {
            ProducerRecord<String, String> producerRecord = testDriver.readOutput(EnumBranchTopology.OUTPUT_DEF, stringDeserializer, stringDeserializer);
            OutputVerifier.compareKeyValue(producerRecord, "two", "delta");
        }
        {
            ProducerRecord<String, String> producerRecord = testDriver.readOutput(EnumBranchTopology.OUTPUT_OTHER, stringDeserializer, stringDeserializer);
            OutputVerifier.compareKeyValue(producerRecord, "three", "tango");
        }
    }
}