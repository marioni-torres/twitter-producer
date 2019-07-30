package com.github.maritorr.twitter.kafka.infrastructure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class ProducerWithCallback {

    private static final Logger LOG = LoggerFactory.getLogger(ProducerWithCallback.class);
    private KafkaProducer producer;
    private Properties properties;
    private ObjectMapper objectMapper;

    public ProducerWithCallback() {
        try {
            setUp();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void setUp() throws IOException {
        properties = new Properties();
        properties.load(ProducerWithCallback.class.getResourceAsStream("/kafka-producer.properties"));

        producer = new KafkaProducer(properties);
        objectMapper = new ObjectMapper();
    }

    public void teardown() {
        if (producer != null) {
            LOG.info("Kafka Producer shutting down...");
            producer.close();
        }
    }

    public void publish(String messageKey, String messageValue) {
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<String, String>(
                        properties.getProperty("twitter.producer.topic"),
                        //messageKey, // TODO - Maybe use a generated key on the other side
                        messageValue);
        producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
            if (exception == null) {
                // TODO - maybe more to do...
                try {
                    LOG.info("Twit *** {}",
                            objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectMapper.readValue(messageValue, Object.class)));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    LOG.error("Could not prettify json", e);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                LOG.error("Something went wrong, no message published", exception);
            }
        });

        producer.flush();
    }
}
