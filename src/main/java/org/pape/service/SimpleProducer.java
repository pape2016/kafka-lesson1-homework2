package org.pape.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleProducer {

    public void send() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.1.26:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "1");
        properties.put("retries", "3");
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 5);
        properties.put("buffer.memory", 33554432);
        properties.put("max.block.ms", 3000);

        Producer<String, String> producer = new KafkaProducer<>(properties);
        try {
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<String, String>("mytopic", Integer.toString(i), Integer.toString(i)));
            }
        } finally {
            producer.close();
        }
    }

    public static void main(String[] args) {
        new SimpleProducer().send();
    }

}
