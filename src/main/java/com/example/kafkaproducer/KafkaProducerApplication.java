package com.example.kafkaproducer;

import org.apache.kafka.clients.producer.*;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.convert.DurationStyle;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Properties;

@SpringBootApplication
public class KafkaProducerApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        Properties pro = new Properties();
        pro.setProperty("bootstrap.servers", "localhost:9092, localhost:9093, localhost:9094");
        pro.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pro.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //Extract One
        pro.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        pro.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        pro.setProperty(ProducerConfig.ACKS_CONFIG, "all");


        Producer<String, String> producer = new KafkaProducer(pro);

        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("hello", "spring hello consumer" + i);
            //send event async with callback
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("done sent " + record.value() + " at offset " + metadata.offset() + " in partition: " + metadata.partition());
                } else {
                    System.out.println(exception.getMessage());
                }
            });
        }

        System.out.println("produce done");
        producer.close(Duration.ofMinutes(1));
    }
}
