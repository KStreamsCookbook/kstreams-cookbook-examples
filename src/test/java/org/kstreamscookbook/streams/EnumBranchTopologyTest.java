package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.Properties;
import java.util.function.Supplier;

class EnumBranchTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new BranchEnumTopology();
    }

    @Test
    void testCopied() {
        var stringSerializer = new StringSerializer();
        var factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

        testDriver.pipeInput(factory.create(BranchEnumTopology.INPUT_TOPIC, "one", "alpha"));
        testDriver.pipeInput(factory.create(BranchEnumTopology.INPUT_TOPIC, "two", "delta"));
        testDriver.pipeInput(factory.create(BranchEnumTopology.INPUT_TOPIC, "three", "tango"));

        var stringDeserializer = new StringDeserializer();

        OutputVerifier.compareKeyValue(testDriver.readOutput(BranchEnumTopology.OUTPUT_ABC, stringDeserializer, stringDeserializer), "one", "alpha");
        OutputVerifier.compareKeyValue(testDriver.readOutput(BranchEnumTopology.OUTPUT_DEF, stringDeserializer, stringDeserializer), "two", "delta");
        OutputVerifier.compareKeyValue(testDriver.readOutput(BranchEnumTopology.OUTPUT_OTHER, stringDeserializer, stringDeserializer), "three", "tango");
    }
}