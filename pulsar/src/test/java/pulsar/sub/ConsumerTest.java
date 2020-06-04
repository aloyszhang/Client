package pulsar.sub;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
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
