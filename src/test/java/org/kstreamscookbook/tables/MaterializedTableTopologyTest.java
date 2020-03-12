package org.kstreamscookbook.tables;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class MaterializedTableTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new MaterializedTableTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    void testCopied() {
        var stringSerializer = new StringSerializer();
        var inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);

        // NOTE: you have to keep using the topic name when sending String keys to distinguish between
        // factory.create(K, V) and factory.create(topicName:String, V)
        // otherwise you can set the topic name when creating the ConsumerRecordFactory
        inputTopic.pipeInput("a", "one");
        inputTopic.pipeInput("b", "one");
        inputTopic.pipeInput("a", "two");

        var stringDeserializer = new StringDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", "one"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", "one"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", "two"));

        // check that the underlying state store contains the latest value for each key
        KeyValueStore<String, String> store = testDriver.getKeyValueStore("my-table");
        assertThat("two").isEqualTo(store.get("a"));
        assertThat("one").isEqualTo(store.get("b"));

        // pass in a tombstone
        inputTopic.pipeInput("b", (String) null);

        assertThat(store.get("b")).isNull(); // no record exists for the key in the state store
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", null)); // the change was emitted as expected
    }
}