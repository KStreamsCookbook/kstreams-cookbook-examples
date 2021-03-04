package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class CountTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new CountTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void testCounts() {
        var stringSerializer = new StringSerializer();
        var inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);

        inputTopic.pipeInput("a", "one");
        inputTopic.pipeInput("b", "one");
        inputTopic.pipeInput("b", "two");
        inputTopic.pipeInput("a", "two");
        inputTopic.pipeInput("a", "three");
        inputTopic.pipeInput("b", "three");

        var stringDeserializer = new StringDeserializer();
        var longDeserializer = new LongDeserializer();

        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, longDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 1L));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", 1L));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", 2L));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 2L));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 3L));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", 3L));
    }
}