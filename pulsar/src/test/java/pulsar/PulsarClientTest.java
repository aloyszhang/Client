package pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.junit.Test;

public class PulsarClientTest {

    public static final String topic = "test/test/auto-topic-4p";
    public static final String serviceUrl = "puslar://localhost:6650";

    protected static PulsarClient pulsarClient;

    @Test
    public void buildPulsarClientTest() throws Exception {
        pulsarClient = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .enableTcpNoDelay(true)
                .build();
    }

}
