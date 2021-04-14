package kafka;

import java.util.List;

public class Topic {
        int partitions;
        int subscriptions;
        double outMsg;
        double inMsg;
        double outBytes;
        double inBytes;
        String topic;
        String persistent;
        int producers;
        long storageSize;
        List<Cluster> clusters;

    public Topic(int partitions, int subscriptions, double outMsg, double inMsg, double outBytes, double inBytes, String topic, String persistent, int producers, long storageSize, List<Cluster> clusters) {
        this.partitions = partitions;
        this.subscriptions = subscriptions;
        this.outMsg = outMsg;
        this.inMsg = inMsg;
        this.outBytes = outBytes;
        this.inBytes = inBytes;
        this.topic = topic;
        this.persistent = persistent;
        this.producers = producers;
        this.storageSize = storageSize;
        this.clusters = clusters;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public int getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(int subscriptions) {
        this.subscriptions = subscriptions;
    }

    public double getOutMsg() {
        return outMsg;
    }

    public void setOutMsg(double outMsg) {
        this.outMsg = outMsg;
    }

    public double getInMsg() {
        return inMsg;
    }

    public void setInMsg(double inMsg) {
        this.inMsg = inMsg;
    }

    public double getOutBytes() {
        return outBytes;
    }

    public void setOutBytes(double outBytes) {
        this.outBytes = outBytes;
    }

    public double getInBytes() {
        return inBytes;
    }

    public void setInBytes(double inBytes) {
        this.inBytes = inBytes;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPersistent() {
        return persistent;
    }

    public void setPersistent(String persistent) {
        this.persistent = persistent;
    }

    public int getProducers() {
        return producers;
    }

    public void setProducers(int producers) {
        this.producers = producers;
    }

    public long getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(long storageSize) {
        this.storageSize = storageSize;
    }

    public List<Cluster> getClusters() {
        return clusters;
    }

    public void setClusters(List<Cluster> clusters) {
        this.clusters = clusters;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Topic{");
        sb.append("partitions=").append(partitions);
        sb.append(", subscriptions=").append(subscriptions);
        sb.append(", outMsg=").append(outMsg);
        sb.append(", inMsg=").append(inMsg);
        sb.append(", outBytes=").append(outBytes);
        sb.append(", inBytes=").append(inBytes);
        sb.append(", topic='").append(topic).append('\'');
        sb.append(", persistent='").append(persistent).append('\'');
        sb.append(", producers=").append(producers);
        sb.append(", storageSize=").append(storageSize);
        sb.append(", clusters=").append(clusters);
        sb.append('}');
        return sb.toString();
    }
}
