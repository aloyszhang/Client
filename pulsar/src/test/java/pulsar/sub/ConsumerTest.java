package pulsar.sub;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.junit.Before;
import org.junit.Test;
import pulsar.PulsarClientTest;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerTest extends PulsarClientTest {
    public static final String serviceUrl = "localhost:6650";
    final AtomicInteger successCounter = new AtomicInteger();
    final AtomicInteger failedCounter = new AtomicInteger();

    @Before
    public void init () throws Exception{
        super.buildPulsarClientTest();;
    }
    @Test
    public void testSubCompact() throws Exception {
        String subName = "test-sub-group";
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName)
                .readCompacted(true)
                .subscribe();
        while(true) {
            Message<byte []> message = consumer.receive();
            String key = message.getKey();
            String value = new String((byte[])message.getValue());
            System.out.println(message.getMessageId());
            consumer.acknowledge(message);
            System.out.println("Receive msg key : "+ key + " value: " + value );
        }
    }
    @Test
    public void testConsumeAckCumulative() throws Exception {
        String subName = "sub";
        Consumer<byte[]> consumer  = pulsarClient.newConsumer()
                .topic("test/test/simple")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
        consumer.seek(new MessageIdImpl(239, 0, 0));

        for (int i = 0 ; i < 200; i++) {
            Message<byte[]> message = consumer.receive();
            System.out.println(((MessageIdImpl) message.getMessageId()) + " : " +  new String(message.getValue()));
            if ( i == 90) {
                consumer.acknowledgeCumulative(message);
            }
        }
        consumer.close();
    }

    @Test
    public void testAckIndividual() throws Exception {
        String subName = "sub";
        Consumer<byte[]> consumer  = pulsarClient.newConsumer()
                .topic("test/test/simple")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
        consumer.seek(new MessageIdImpl(239, 0, 0));

        for (int i = 0 ; i < 200; i++) {
            Message<byte[]> message = consumer.receive();
            System.out.println(((MessageIdImpl) message.getMessageId()) + " : " +  new String(message.getValue()));
            if ( i % 10 != 0) {
                consumer.acknowledge(message);
            }
        }
        consumer.close();
    }

    @Test
    public void testSimpleConsume() throws Exception {
        String subName = "sub";
        Consumer<byte[]> consumer  = pulsarClient.newConsumer()
                .topic("test/test/simple")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();


        for (int i = 0 ; i < 200; i++) {
            Message<byte[]> message = consumer.receive();
            System.out.println(((MessageIdImpl) message.getMessageId()) + " : " +  new String(message.getValue()));
        }
        consumer.close();
    }

    @Test
    public void testSub() throws Exception {
        boolean async = false;

        String subName = "test-sub-group";
        Consumer<byte[]> consumer;
        try {
            consumer = pulsarClient.newConsumer()
                    .topic(topic)//"tenantTest/namespaceTest/topicTest"
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                    .subscriptionName(subName)
                    .subscribe();
        } catch (PulsarClientException e) {
            e.printStackTrace();
            return;
        }


        while(true) {
            if(async) {
                CompletableFuture<Message<byte[]>> future = consumer.receiveAsync();
                future.whenCompleteAsync((m, t) -> {
                    if(t != null) {
                        t.printStackTrace();
                        if(t.getMessage().contains("Consumer already closed")) {
                            return;
                        } else {
                            t.printStackTrace();
                        }
                        failedCounter.incrementAndGet();
                        return;
                    }
                    System.out.println(m);
                    int sum = successCounter.incrementAndGet();
                    if(sum % 1000 == 0) {
                        System.out.println("consume : " + sum);
                    }
                });
            } else {
                System.out.println("=====");
                try {
                    Message<byte[]> msg = consumer.receive();
                    consumer.acknowledge(msg);
                    int sum = successCounter.incrementAndGet();
                    TopicMessageIdImpl messageId = (TopicMessageIdImpl) msg.getMessageId();
                    System.out.println("Recieved :" + messageId.getTopicPartitionName() + " " +
                            messageId.getInnerMessageId());
                    if (sum % 1000 == 0) {
                        System.out.println("consume : " + sum);
                    }
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                    if(e.getMessage().contains("Consumer already closed")) {
                        break;
                    } else {
                        e.printStackTrace();
                    }
                    failedCounter.incrementAndGet();
                }
            }
        }

    }

}
