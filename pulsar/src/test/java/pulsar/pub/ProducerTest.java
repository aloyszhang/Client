package pulsar.pub;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pulsar.PulsarClientTest;

import java.util.concurrent.atomic.AtomicLong;

public class ProducerTest extends PulsarClientTest {
    public static final Logger LOG = LoggerFactory.getLogger(ProducerTest.class);

    public static AtomicLong counter = new AtomicLong(0);
    private static Producer<byte [] > producer;

    @Before
    public void setup() throws Exception {
        super.buildPulsarClientTest();
        producer = pulsarClient.newProducer()
                .topic(topic)
                .create();
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
        MessageIdImpl messageId = (MessageIdImpl) producer.newMessage()
                .key("Key-async-" + counter.get() % 3)
                .value(("Value-async-" + counter.get()).getBytes("utf-8"))
                .property("myKey-async","myValue-async")
                .sendAsync().get();
        LOG.info( "Send message async success, ledgerId {}, endtry id {},  partition id {} ",
                new Object[]{messageId.getLedgerId(), messageId.getEntryId(), messageId.getPartitionIndex()});
    }
}
