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

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";

    @Override
    protected Supplier<Topology> withTopology() {
        return new FlatMapValuesTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void testMapping() {
        var stringSerializer = new StringSerializer();
        var inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);

        inputTopic.pipeInput("key", "This is a  sentence");

        expectNextKVPair("key", "This");
        expectNextKVPair("key", "is");
        expectNextKVPair("key", "a");
        expectNextKVPair("key", "sentence");
    }

    // TODO refactor this out to make it consistent with other tests
    private void expectNextKVPair(String k, String v) {
        var stringDeserializer = new StringDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>(k,v));
    }
}
