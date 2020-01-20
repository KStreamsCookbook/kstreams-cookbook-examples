package org.kstreamscookbook.app;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.kstreamscookbook.streams.windowed.SessionWindowedAggregateTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SessionWindowApp {
    private static Logger LOG = LoggerFactory.getLogger(SessionWindowApp.class);

    public static String INPUT_TOPIC = "input_topic";
    public static String OUTPUT_TOPIC = "output-topic";

    public static void main(String[] args) {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-window");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // disable caching to see session merging
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        Duration duration = Duration.ofSeconds(7);

        var topology = new SessionWindowedAggregateTopology(INPUT_TOPIC, OUTPUT_TOPIC, duration).get();
        var kafkaStreams = new KafkaStreams(topology, props);

        var latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down app now");
            kafkaStreams.close();

            System.out.println("Closing Kafka Streams ...");

            latch.countDown();
        }));

        try {
            System.out.println("Clearing out Kafka Streams before start ...");
            kafkaStreams.cleanUp();
            System.out.println("Starting Kafka Streams ...");
            kafkaStreams.start();
            latch.await();
        } catch (Throwable t) {
            System.exit(1);
        }
        System.exit(0);

    }
}
