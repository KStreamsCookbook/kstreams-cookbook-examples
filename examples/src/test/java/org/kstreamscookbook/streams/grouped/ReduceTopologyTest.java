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

class ReduceTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new ReduceTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void testReduce() {
        var stringSerializer = new StringSerializer();
        var inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);

        inputTopic.pipeInput("a", "1");
        inputTopic.pipeInput("b", "2");
        inputTopic.pipeInput("b", "3");
        inputTopic.pipeInput("a", "4");
        inputTopic.pipeInput("a", "5");
        inputTopic.pipeInput("b", "6");

        var stringDeserializer = new StringDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", "1"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", "2"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", "2|3"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", "1|4"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", "1|4|5"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", "2|3|6"));
    }
}