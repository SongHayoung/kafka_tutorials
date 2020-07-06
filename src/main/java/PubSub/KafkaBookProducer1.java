package PubSub;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaBookProducer1 {
    public static void main(String[] args) {
        plainSendKafka();
        syncSendKafka();
        asyncSendKafka();
        plainSendKafkaWithOptions();
    }

    private static void plainSendKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        try {
            producer.send(new ProducerRecord<String, String>("test", "Plain Send to Kafka"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static void syncSendKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        try {
            RecordMetadata metadata = producer.send(new ProducerRecord<String, String>("test", "Sync Send to Kafka")).get();
            System.out.printf("Partition : %d, Offset : %d\n", metadata.partition(), metadata.offset());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static void asyncSendKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        try {
            producer.send(new ProducerRecord<String, String>("test", "Async Send to Kafka"), new kafkaCallback());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    static class kafkaCallback implements Callback {
        public void onCompletion(RecordMetadata metadata, Exception e) {
            if (metadata != null) {
                System.out.println("Partition : " + metadata.partition() + ", Offset : " + metadata.offset() + "");
            } else {
                e.printStackTrace();
            }
        }
    }

    private static void plainSendKafkaWithOptions() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "1");
        props.put("compression.type", "gzip");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        try {
            producer.send(new ProducerRecord<String, String>("test", "Plain Send to Kafka With Options"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
