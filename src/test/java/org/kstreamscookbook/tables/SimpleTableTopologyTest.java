package org.kstreamscookbook.tables;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class SimpleTableTopologyTest extends TopologyTestBase {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    @Override
    protected Supplier<Topology> withTopology() {
        return new SimpleTableTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    void testCopied() {

        var stringSerializer = new StringSerializer();
        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);

        var stringDeserializer = new StringDeserializer();
        TestOutputTopic<String, String> outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        inputTopic.pipeInput("a", "one");
        inputTopic.pipeInput("b", "one");
        inputTopic.pipeInput("a", "two");

        assertNextKVPair(outputTopic, "a", "one");
        assertNextKVPair(outputTopic, "b", "one");
        assertNextKVPair(outputTopic,"a", "two");
    }

    private void assertNextKVPair(TestOutputTopic<String, String> outputTopic, String k, String v) {
        var record = outputTopic.readRecord();
        assertThat(record.getKey()).isEqualTo(k);
        assertThat(record.getValue()).isEqualTo(v);
    }

}