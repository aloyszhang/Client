package kafka;

public class Cluster {
    String topic;
    int partitions;
    String persistent;
    int producerCount;
    int subscriptionCount;
    double msgRateIn;
    double msgThroughputIn;
    double msgRateOut;
    double msgThroughputOut;
    double averageMsgSize;
    long storageSize;

    public Cluster(String topic, int partitions, String persistent, int producerCount, int subscriptionCount, double msgRateIn, double msgThroughputIn, double msgRateOut, double msgThroughputOut, double averageMsgSize, long storageSize) {
        this.topic = topic;
        this.partitions = partitions;
        this.persistent = persistent;
        this.producerCount = producerCount;
        this.subscriptionCount = subscriptionCount;
        this.msgRateIn = msgRateIn;
        this.msgThroughputIn = msgThroughputIn;
        this.msgRateOut = msgRateOut;
        this.msgThroughputOut = msgThroughputOut;
        this.averageMsgSize = averageMsgSize;
        this.storageSize = storageSize;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public String getPersistent() {
        return persistent;
    }

    public void setPersistent(String persistent) {
        this.persistent = persistent;
    }

    public int getProducerCount() {
        return producerCount;
    }

    public void setProducerCount(int producerCount) {
        this.producerCount = producerCount;
    }

    public int getSubscriptionCount() {
        return subscriptionCount;
    }

    public void setSubscriptionCount(int subscriptionCount) {
        this.subscriptionCount = subscriptionCount;
    }

    public double getMsgRateIn() {
        return msgRateIn;
    }

    public void setMsgRateIn(double msgRateIn) {
        this.msgRateIn = msgRateIn;
    }

    public double getMsgThroughputIn() {
        return msgThroughputIn;
    }

    public void setMsgThroughputIn(double msgThroughputIn) {
        this.msgThroughputIn = msgThroughputIn;
    }

    public double getMsgRateOut() {
        return msgRateOut;
    }

    public void setMsgRateOut(double msgRateOut) {
        this.msgRateOut = msgRateOut;
    }

    public double getMsgThroughputOut() {
        return msgThroughputOut;
    }

    public void setMsgThroughputOut(double msgThroughputOut) {
        this.msgThroughputOut = msgThroughputOut;
    }

    public double getAverageMsgSize() {
        return averageMsgSize;
    }

    public void setAverageMsgSize(double averageMsgSize) {
        this.averageMsgSize = averageMsgSize;
    }

    public long getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(long storageSize) {
        this.storageSize = storageSize;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Cluster{");
        sb.append("topic='").append(topic).append('\'');
        sb.append(", partitions=").append(partitions);
        sb.append(", persistent='").append(persistent).append('\'');
        sb.append(", producerCount=").append(producerCount);
        sb.append(", subscriptionCount=").append(subscriptionCount);
        sb.append(", msgRateIn=").append(msgRateIn);
        sb.append(", msgThroughputIn=").append(msgThroughputIn);
        sb.append(", msgRateOut=").append(msgRateOut);
        sb.append(", msgThroughputOut=").append(msgThroughputOut);
        sb.append(", averageMsgSize=").append(averageMsgSize);
        sb.append(", storageSize=").append(storageSize);
        sb.append('}');
        return sb.toString();
    }
}
