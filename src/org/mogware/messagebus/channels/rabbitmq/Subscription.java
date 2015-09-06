package org.mogware.messagebus.channels.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import java.io.IOException;
import org.mogware.system.Disposable;
import org.mogware.system.threading.TimeSpan;

public class Subscription implements Disposable {
    private final Channel channel;
    private String consumerTag;
    private Delivery latestEvent;

    private volatile QueueingConsumer consumer;

    public Subscription(Channel channel,
            RabbitChannelGroupConfiguration config) {
        this.channel = channel;
        this.consumer = new QueueingConsumer(this.channel);
        try {
            this.consumerTag = this.channel.basicConsume(
                    config.getInputQueue(),
                    config.getTransactionType() == RabbitTransactionType.None,
                    this.consumer);
        } catch (IOException ex) {
        }
        this.latestEvent = null;
    }

    public Delivery BeginReceive(TimeSpan timeout) {
        QueueingConsumer consumer = this.consumer;
        try {
            if (consumer == null || !this.channel.isOpen())
                this.mutateLatestEvent(null);
            else {
                Delivery delivery = consumer.nextDelivery(
                        (long) timeout.getTotalMilliseconds());
                this.mutateLatestEvent(delivery);
            }
        } catch (Exception ex) {
            this.mutateLatestEvent(null);
        }
        return this.latestEvent;
    }

    public void AcknowledgeMessages() {
        Delivery delivery = this.latestEvent;
        long tag = delivery == null ? 0 :
                delivery.getEnvelope().getDeliveryTag();
        try {
            this.channel.basicAck(tag, true);
        } catch (IOException ex) {
        }
    }

    protected void mutateLatestEvent(Delivery delivery) {
        synchronized(this) {
            this.latestEvent = delivery;
        }
    }

    @Override
    public void dispose() {
        try {
            boolean shouldCancelConsumer = false;
            if (this.consumer != null)
                shouldCancelConsumer = true;
            this.consumer = null;
            if (shouldCancelConsumer && this.channel.isOpen())
                this.channel.basicCancel(this.consumerTag);
            this.consumerTag = null;
        } catch (IOException ex) {
        }
    }
}
