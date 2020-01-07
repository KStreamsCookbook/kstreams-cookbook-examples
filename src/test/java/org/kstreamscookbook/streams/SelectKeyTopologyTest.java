package org.kstreamscookbook.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.util.function.Supplier;

public class SelectKeyTopologyTest extends TopologyTestBase {

  public static final String INPUT_TOPIC = "input-topic";
  public static final String OUTPUT_TOPIC = "output-topic";

  @Override
  protected Supplier<Topology> withTopology() {
    return new SelectKeyTopology(INPUT_TOPIC, OUTPUT_TOPIC);
  }

  @Test
  public void testSelectKey() {
    var stringSerializer = new StringSerializer();
    var factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);
    testDriver.pipeInput(factory.create(INPUT_TOPIC, "key", "newkey:value"));

    var stringDeserializer = new StringDeserializer();
    OutputVerifier.compareKeyValue(testDriver.readOutput(OUTPUT_TOPIC, stringDeserializer, stringDeserializer), "newkey", "newkey:value");
  }
}