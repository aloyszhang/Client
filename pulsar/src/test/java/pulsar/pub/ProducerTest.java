package pulsar.pub;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pulsar.PulsarClientTest;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ProducerTest extends PulsarClientTest {
    public static final Logger LOG = LoggerFactory.getLogger(ProducerTest.class);

    public static AtomicLong counter = new AtomicLong(0);
    private static Producer<byte [] > producer;

    String iWill = "IWill";
    String lu = "_love_u_for_everyday";
   // @Before
    public void setup() throws Exception {
        super.buildPulsarClientTest();
        producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(false)
                .create();
    }
    @Test
    public void testSendKey() throws Exception {

        int total = 300;
        int keyNum = 3;
        AtomicLong counter = new AtomicLong(900);
        for ( int i = 0; i < total; i ++) {
            String key = "Key-" + (i % keyNum);
            byte [] value = ("Value for " + key + ", number-" + counter.getAndIncrement()).getBytes("utf-8");
            TypedMessageBuilder<byte []> msgbuilder = producer.newMessage()
                    .key(key)
                    .value(value);
            MessageIdImpl messageId = (MessageIdImpl) msgbuilder.send();
            System.out.println("Send key:  " + key + " value : " + new String(value) + " success " +
                    " and msgId is : " + messageId);
        }
    }

    @Test
    public void testSend () throws Exception {
        counter.incrementAndGet();
        MessageIdImpl messageId = (MessageIdImpl) producer.newMessage()
                .key("Key-" + counter.get() % 3)
                .value(("Value-" + counter.get()).getBytes("utf-8"))
                .property("mykey","myvalue")
                .send();
        LOG.info( "Send message success, ledgerId {}, endtry id {},  partition id {} ",
                new Object[]{messageId.getLedgerId(), messageId.getEntryId(), messageId.getPartitionIndex()});
    }

    @Test
    public void testSendAsync() throws Exception {
        counter.incrementAndGet();
        TypedMessageBuilder<byte []> messageBuilder = producer.newMessage()
                .key("Key-async-" + counter.get() % 3)
                .value(("Value-async-" + counter.get()).getBytes("utf-8"))
                .property("myKey-async","myValue-async");
        MessageIdImpl messageId = (MessageIdImpl) messageBuilder.sendAsync().get();
        LOG.info( "Send message async success, ledgerId {}, endtry id {},  partition id {} ",
                new Object[]{messageId.getLedgerId(), messageId.getEntryId(), messageId.getPartitionIndex()});
    }
    @Test
    public void dupRateTest() throws Exception {
        rateTest();
    }

    @Test
    public void sendSimple() throws Exception {
        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://127.0.0.1:6650")//"pulsar://10.215.128.78:16650"
                .build();
        String topic = "test/test/simple";
        int msgCount = 100;

        Producer producer = client.newProducer()
                .topic(topic)
                .producerName("Simple-producer")
                .blockIfQueueFull(true)
                .enableBatching(false)
                .maxPendingMessages(1000)
                .create();
        for (int i = 0 ; i < msgCount; i++) {
            byte [] msg = ("Message " + i).getBytes("utf-8");
            MessageIdImpl messageId = (MessageIdImpl) producer.send(msg);
            System.out.println(messageId);
        }


    }

    @Test
    public void rateTest() throws Exception {
        final int msgLength = 1024;
        final boolean async = true;
        final int sleepMs = 1000;

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://127.0.0.1:6650")//"pulsar://10.215.128.78:16650"
                .build();
        // final ExecutorService pool = Executors.newFixedThreadPool(producerCount);
        final AtomicInteger successCounter = new AtomicInteger();
        final AtomicInteger isFullCounter = new AtomicInteger();
        final AtomicInteger timeoutCounter = new AtomicInteger();
        final AtomicInteger failedCounter = new AtomicInteger();
        StringBuffer msgSb = new StringBuffer(msgLength);
        for(int c = 0; c < msgLength;c++) {
            msgSb.append('a');
        }
        final String msg = msgSb.toString();
        List<Producer<byte[]>> producerList = new LinkedList<>();
        final Producer<byte[]> producer = client.newProducer()
                .topic(topic)//"tenantTest/namespaceTest/topicTest"
                .maxPendingMessages(5000)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();
        producerList.add(producer);


        new Thread(() -> {
            while(true) {
                for(int i = 0;i < producerList.size();i++) {
                    try {
                        /*ProducerStats producerStats = producerList.get(i).getStats();
                        System.out.println("#### producer : " + producerStats.getTotalMsgsSent() + ", " + producerStats.getTotalSendFailed()
                                + ", " + producerStats.getTotalAcksReceived() );*/
                        System.out.println("### producer : " + successCounter.get() + ", isFullCounter : " + isFullCounter.get() + ", timeout : " + timeoutCounter.get() + ", fail : " + failedCounter.get());
                        successCounter.set(0);
                        isFullCounter.set(0);
                        timeoutCounter.set(0);
                        failedCounter.set(0);
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();


        // pool.submit(() -> {
        if (async) {
            final AtomicLong sleep = new AtomicLong(0);
            final AtomicBoolean isBreak = new AtomicBoolean(false);
            for (int j = 0;true && !isBreak.get();j++) {
                // You can then send messages to the broker and topic you specified:
                try {
                    Thread.sleep(sleep.get());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                CompletableFuture<MessageId> future = producer.sendAsync(msg.getBytes());
                future.whenCompleteAsync((m, t) -> {
                    if (t != null) {
                        if(t.getMessage().contains("Producer send queue is full") ) {
                            System.out.println("Producer send queue is full");
                            isFullCounter.incrementAndGet();
                            sleep.set(sleepMs);
                            return;
                        } else if (t.getMessage().contains("Could not send message to broker within given timeout")) {
                            System.out.println("Send timeout");
                            timeoutCounter.incrementAndGet();
                            sleep.set(sleepMs);
                            return;
                        } else if (t.getMessage().contains("Producer already closed")) {
                            System.out.println("Producer closed.");
                            isBreak.set(true);
                            return;
                        } else {
                            System.out.println("Unkonwn error.");
                            t.printStackTrace();
                        }
                        failedCounter.addAndGet(1);
                        return;
                    } else {
                        sleep.set(0);
                    }
                    successCounter.incrementAndGet();
                });
            }
        } else {
            while(true) {
                try {
                    MessageIdImpl messageId = (MessageIdImpl) producer.send(msg.getBytes());
                    successCounter.incrementAndGet();
                    System.out.println("MessageId: " + messageId);

                } catch (PulsarClientException e) {
                    /*System.out.println("Error, " + e);
                    failedCounter.incrementAndGet();
                    e.printStackTrace();
                    if(e.getMessage().contains("Producer send queue is full") ) {
                        System.out.println("Sync Producer send queue is full");

                        isFullCounter.incrementAndGet();
                        try {
                            Thread.sleep(sleepMs);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                        continue;
                    } else if (e.getMessage().contains("Could not send message to broker within given timeout")) {
                        System.out.println("Sync timeout");

                        timeoutCounter.incrementAndGet();
                        try {
                            Thread.sleep(sleepMs);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                        continue;
                    } else if (e.getMessage().contains("Producer already closed")) {
                        System.out.println("Sync Producer closed");
                        break;
                    }
                    e.printStackTrace();*/
                    failedCounter.addAndGet(1);
                }
            }
        }
    }

}
