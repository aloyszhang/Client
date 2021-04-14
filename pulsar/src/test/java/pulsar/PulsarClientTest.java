package pulsar;

import org.apache.pulsar.client.api.PulsarClient;
import org.junit.BeforeClass;
import org.junit.Test;

public class PulsarClientTest {

    public static final String topic = "public/default/rc";
    public static final String serviceUrl = "puslar://localhost:6650";

    protected static PulsarClient pulsarClient;

    @BeforeClass
    public static void buildPulsarClientTest() throws Exception {
        pulsarClient = PulsarClient.builder()
                .serviceUrl(serviceUrl)
                .enableTcpNoDelay(true)
                .build();
    }

}
