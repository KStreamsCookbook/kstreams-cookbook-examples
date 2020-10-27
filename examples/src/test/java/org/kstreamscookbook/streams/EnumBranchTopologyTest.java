package org.kstreamscookbook.streams;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class EnumBranchTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new BranchEnumTopology();
    }

    @Test
    void testCopied() {
        var stringSerializer = new StringSerializer();

        var inputTopic = testDriver.createInputTopic(BranchTopology.INPUT_TOPIC, stringSerializer,stringSerializer);

        inputTopic.pipeInput("one", "alpha");
        inputTopic.pipeInput("two", "delta");
        inputTopic.pipeInput("three", "tango");

        var stringDeserializer = new StringDeserializer();
        var outputTopicABC = testDriver.createOutputTopic(BranchTopology.OUTPUT_ABC, stringDeserializer, stringDeserializer);
        var outputTopicDEF = testDriver.createOutputTopic(BranchTopology.OUTPUT_DEF, stringDeserializer, stringDeserializer);
        var outputTopicOther = testDriver.createOutputTopic(BranchTopology.OUTPUT_OTHER, stringDeserializer, stringDeserializer);

        assertThat(outputTopicABC.readKeyValue()).isEqualTo(new KeyValue<>("one","alpha"));
        assertThat(outputTopicDEF.readKeyValue()).isEqualTo(new KeyValue<>("two","delta"));
        assertThat(outputTopicOther.readKeyValue()).isEqualTo(new KeyValue<>("three","tango"));
    }
}