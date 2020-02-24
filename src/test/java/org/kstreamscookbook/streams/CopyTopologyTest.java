package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;


import org.kstreamscookbook.TopologyTestBase;

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
        var stringDeserializer = new StringDeserializer();

        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer,stringSerializer);
        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        inputTopic.pipeInput("key", "value");

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key", "value"));
        assertThat(outputTopic.isEmpty()).isTrue();
    }
}