package kafka;

import java.util.List;

public class Meta {
    int total;
    List<Topic> topics;

    public Meta(int total, List<Topic> topics) {
        this.total = total;
        this.topics = topics;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public List<Topic> getTopics() {
        return topics;
    }

    public void setTopics(List<Topic> topics) {
        this.topics = topics;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Meta{");
        sb.append("total=").append(total);
        sb.append(", topics=").append(topics);
        sb.append('}');
        return sb.toString();
    }
}
