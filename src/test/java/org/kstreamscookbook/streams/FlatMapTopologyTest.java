package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
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

public class FlatMapTopologyTest extends TopologyTestBase {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";

    @Override
    protected Supplier<Topology> withTopology() {
        return new FlatMapTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void testMapping() {
        var stringSerializer = new StringSerializer();
        var inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);

        inputTopic.pipeInput("key", "This is a  sentence");

        var stringDeserializer = new StringDeserializer();
        var integerDeserializer = new IntegerDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, integerDeserializer, stringDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(0, "This"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(1, "is"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(2, "a"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(3, "sentence"));
    }
}

