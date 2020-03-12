package org.kstreamscookbook.tables;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.security.Key;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class SimpleTableTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new SimpleTableTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    void testCopied() {

        var stringSerializer = new StringSerializer();
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);

        var stringDeserializer = new StringDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        inputTopic.pipeInput("a", "one");
        inputTopic.pipeInput("b", "one");
        inputTopic.pipeInput("a", "two");

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", "one"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", "one"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", "two"));
    }

}