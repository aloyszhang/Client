package kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;


public class KafkaProducerTests {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerTests.class);
    private static final String bootstrapServer = "localhost:9092";
    private static final String topic = "index";

    private static KafkaProducer<byte[], byte[]> producer;

    @BeforeClass
    public static void setup() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new org.apache.kafka.clients.producer.KafkaProducer(properties);
    }


    @Test
    public void testSend() throws Exception {
        try {
            for (int i = 0; i < 100; i ++) {
                    byte[] baseMsg = getMessageBody(10).getBytes("utf-8");
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord(topic, baseMsg);
                    RecordMetadata recordMetadata = producer.send(record).get();
                    System.out.println("### recordMeta " + recordMetadata.offset() + " " + recordMetadata.timestamp());
            }
        } catch (Throwable e) {
            log.error("Send error,", e);
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }

    private static String getMessageBody(int length) {
        String base = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        Random random = new Random(base.length());
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            sb.append(base.charAt(random.nextInt(base.length())));
        }
        sb.append(new Date());
        log.info("BaseMsg length : " + sb.length());
        return sb.toString();
    }

    @Test
    public void testSASLProducer() throws Exception {
        String topic = "test";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.160.129.190:8080");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + "user01" + "\" password=\"" + "user01" + "\";");
        KafkaProducer<byte[], byte[]> producer = new org.apache.kafka.clients.producer.KafkaProducer(properties);


        try {
            for (int i = 0; i < 100; i ++) {
                byte[] baseMsg =("Message-" + i).getBytes("utf-8");
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, baseMsg);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Send exception", exception);
                    } else {
                        log.info("Send msg success {} at time {} with timestamp {}", new Object[]{
                                new String(record.value()), System.currentTimeMillis() ,metadata.timestamp()});
                    }
                });
            }
            Thread.sleep(300 * 1000 );
        } catch (Throwable e) {
            log.error("Send error,", e);
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }


    @Test
    public void testSkipMap() throws Exception {
        NavigableMap<Integer, Integer> map = new ConcurrentSkipListMap<>();
        map.put(1, 1);
        map.put(2,2);
        map.put(4,4);
        System.out.println(map.floorEntry(1));
        System.out.println(map.floorEntry(2));
        System.out.println(map.floorEntry(3));
    }





}
