package kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.BeforeClass;
import org.junit.Test;



public class KafkaConsumerTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerTest.class);

    private static final String bootstrapServer = "localhost:9092";
    private static final String topic = "single";
    private static final String consumerGroup = "sub-test-kafka-002";
     // offsetMode : earliest or latest

    private static KafkaConsumer kafkaConsumer;

    @BeforeClass
    public static void setup() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put("group.id", consumerGroup);
        properties.put("session.timeout.ms", "30000");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaConsumer = new KafkaConsumer(properties);
    }

    @Test
    public void testSimpleConsume() throws Exception {


        kafkaConsumer.subscribe(Collections.singletonList("index"));
        for(int i = 0 ; i < 100; i ++) {
            ConsumerRecords<byte[], byte[]> cRecords = kafkaConsumer.poll(Duration.ofSeconds(3));
            if (cRecords != null && cRecords.count() > 0) {
                for ( ConsumerRecord<byte[], byte[]> record : cRecords) {
                    System.out.println("Key : " + record.key() );
                    System.out.println("Value : " + record.value() );
                    System.out.println("Partition : " + record.partition() );
                    System.out.println("Offset : " + record.offset() );
                }
                kafkaConsumer.commitSync();
            } else {
                System.out.println("No more message ");
                Thread.sleep(1000);
            }
            Thread.sleep(5000);
        }
    }

    @Test
    public void testConsume() throws Exception {


        kafkaConsumer.subscribe(Collections.singletonList("test-kop"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                Map<TopicPartition,Long> beginningOffset = kafkaConsumer.beginningOffsets(collection);

                //读取历史数据 --from-beginning
                for(Map.Entry<TopicPartition,Long> entry : beginningOffset.entrySet()){
                    // 基于seek方法
                    //TopicPartition tp = entry.getKey();
                    //long offset = entry.getValue();
                    //consumer.seek(tp,offset);

                    // 基于seekToBeginning方法
                    kafkaConsumer.seekToBeginning(collection);
                }
            }
        });
        while(true) {
            ConsumerRecords<byte[], byte[]> cRecords = kafkaConsumer.poll(0 );
            if (cRecords != null && cRecords.count() > 0) {
                for ( ConsumerRecord<byte[], byte[]> record : cRecords) {
                    System.out.println(new String(record.value()));
                }
                kafkaConsumer.commitSync();
            } else {
                System.out.println("No more message ");
                Thread.sleep(1000);
            }
        }
    }

    @Test
    public void getOffsetTest () throws Exception {
        getOffsetForTimes(1591941430538l);
        // getOffsetForTimes(System.currentTimeMillis());
    }



    private void getOffsetForTimes(long time) {
        final HashMap<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        // 获取分区信息，并构建分区offset reset请求
        final List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
        log.info("### Topic partition info {} " , partitionInfos);
        final LinkedList<TopicPartition> topicPartitions = new LinkedList<>();
        for (PartitionInfo partitionInfo : partitionInfos) {

            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            timestampsToSearch.put(topicPartition, time);
            topicPartitions.add(topicPartition);
        }
        log.info("### Offset request for topic partitions {} " , timestampsToSearch);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = kafkaConsumer.offsetsForTimes(timestampsToSearch);

        kafkaConsumer.assign(topicPartitions);
        for(Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetAndTimestampMap.entrySet()) {
            log.info("### offset response to seek, topicPartition  {}, offsetAndTime {}" , entry.getKey(), entry.getValue());
            kafkaConsumer.seek(entry.getKey(), entry.getValue().offset());
        }
    }


    @Test
    public void testTimeForOffset() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        long old = sdf.parse("2020-06-08 14:00").getTime();
        getOffsetForTimes(old);
        long mid = sdf.parse("2020-06-10 00:00").getTime();
        getOffsetForTimes(mid);
        getOffsetForTimes(System.currentTimeMillis());
    }


    private long getTimeLong (String time) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        return sdf.parse(time).getTime();
    }

    @Test
    public void testSort() throws Exception {
        CopyOnWriteArrayList<Integer> list = new CopyOnWriteArrayList<>();
        list.add(2);
        list.add(3);
        list.add(1);
        list.add(4);
        list.sort(Comparator.comparingInt(c -> c));
        System.out.println(list);
    }


    @Test
    public void testSASLConsumer() throws Exception {

    }
    public static void main(String[] args) throws InterruptedException {
        String topic = args[0].trim();
        String group = args[1].trim();
        Properties properties = new Properties();


        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.160.129.190:8080");
        properties.put("group.id", group);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + "user01" + "\" password=\"" + "user01" + "\";");
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        while(true) {
            ConsumerRecords<byte[], byte[]> cRecords = kafkaConsumer.poll(0 );
            if (cRecords != null && cRecords.count() > 0) {
                for ( ConsumerRecord<byte[], byte[]> record : cRecords) {
                    System.out.println(new String(record.value()));
                }
                kafkaConsumer.commitSync();
            } else {
                System.out.println("No more message ");
                Thread.sleep(1000);
            }
        }
    }
}
