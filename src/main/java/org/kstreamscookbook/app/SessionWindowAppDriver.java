package org.kstreamscookbook.app;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class SessionWindowAppDriver {

    private String bootstrapServers;
    private CountDownLatch latch = new CountDownLatch(1);
    private boolean consumerRunning = true;
    private boolean producerRunning = true;

    public SessionWindowAppDriver(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public static void main(String[] args) {
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        SessionWindowAppDriver driver = new SessionWindowAppDriver(bootstrapServers);

        driver.run();
    }

    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumerRunning = false;
            producerRunning = false;
            latch.countDown();
        }));

        // Start consumer (in separate thread)
        new Thread(this::consume).start();
        new Thread(this::produce).start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            System.out.println("Producer latch interrupted");
            System.exit(1);
        }

        System.exit(0);
    }

    public void consume() {
        final Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "session-windows-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());

        System.out.println("Hello from the consumer");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(SessionWindowApp.OUTPUT_TOPIC));
        while (consumerRunning) {
            final ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            records.forEach(record -> System.out.println(record.key() + " : " + record.value()));
        }

        consumer.close();
    }

    public void produce() {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        var serial = Serdes.String().serializer();

        System.out.println("Hello from the producer");

        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps, serial, serial);

        // produce one record every few seconds
        var random = new Random();
        var counter  = 0;

        while (producerRunning) {
            producer.send(new ProducerRecord<>(SessionWindowApp.INPUT_TOPIC, "a", counter + "s"));
            int gap = 1 + random.nextInt(9); // up to 10 seconds, 5 is the sessionWindow

            counter += gap;

            try {
                Thread.sleep(gap * 1000);
            } catch (InterruptedException e) {
                System.out.println("Producer interrupted!");
                break;
            }
        }

        producer.close();
    }
}
