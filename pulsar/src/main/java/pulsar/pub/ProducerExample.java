package pulsar.pub;


public class ProducerExample {
    public static void main(String[] args) {
        System.out.println("You should input parameters as follows: " +
                "\nBrokerServerUrls" +
                "\nTopicName" +
                "\nMessageSize" +
                "\nTotalMessageNubmer");
        if (args.length < 4) {
            throw new RuntimeException("You must input parameter as tips.");
        }
        String brokerServiceUrl = args[0].trim();
        String topicName = args[1].trim();
        int messageSize = Integer.parseInt(args[2].trim());
        long totalMessageNumber = Long.parseLong(args[3].trim());


    }
}
