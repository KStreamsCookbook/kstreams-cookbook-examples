package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
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

public class FlatMapValuesTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new FlatMapValuesTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void testMapping() {
        var stringSerializer = new StringSerializer();
        var inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);

        inputTopic.pipeInput("key", "This is a  sentence");

        var stringDeserializer = new StringDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key", "This"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key", "is"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key", "a"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key", "sentence"));
    }
}
