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

public class SelectKeyTopologyTest extends TopologyTestBase {

  @Override
  protected Supplier<Topology> withTopology() {
    return new SelectKeyTopology(INPUT_TOPIC, OUTPUT_TOPIC);
  }

  @Test
  public void testSelectKey() {
    var stringSerializer = new StringSerializer();
    var inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);

    inputTopic.pipeInput("key", "newkey:value");

    var stringDeserializer = new StringDeserializer();
    var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("newkey", "newkey:value"));
  }
}