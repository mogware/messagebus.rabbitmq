package org.mogware.messagebus.channels.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.mogware.messagebus.ChannelConnectionException;
import org.mogware.messagebus.ChannelConnector;
import org.mogware.messagebus.ChannelEnvelope;
import org.mogware.messagebus.ChannelGroupConfiguration;
import org.mogware.messagebus.ChannelMessage;
import org.mogware.messagebus.ChannelTransaction;
import org.mogware.messagebus.ConnectionState;
import org.mogware.messagebus.DeliveryContext;
import org.mogware.messagebus.DependencyResolver;
import org.mogware.messagebus.DispatchContext;
import org.mogware.messagebus.MessagingChannel;
import org.mogware.system.delegates.Action0;
import org.mogware.system.delegates.Action1;
import org.mogware.system.delegates.Func0;

public class RabbitChannel implements MessagingChannel {
    private static final boolean ContinueReceiving = true;
    private static final boolean FinishedReceiving = false;
    private static final AtomicInteger counter = new AtomicInteger(0);

    private final Channel channel;
    private final Connection connection;
    private final ChannelConnector connector;
    private final RabbitMessageAdapter adapter;
    private final RabbitChannelGroupConfiguration config;
    private final RabbitTransactionType transactionType;
    private final Func0<RabbitSubscription> subscriptionFactory;
    private final int identifier;

    private ChannelMessage currentMessage;
    private DependencyResolver currentResolver;
    private ChannelTransaction currentTransaction;
    private ChannelGroupConfiguration currentConfig;

    private RabbitSubscription subscription;
    private Delivery delivery;
    private boolean suppressOperations;
    private boolean disposed;
    private volatile boolean shutdown;

    public RabbitChannel(Channel channel, Connection connection,
            ChannelConnector connector,
            RabbitChannelGroupConfiguration configuration,
            Func0<RabbitSubscription> subscriptionFactory) {
        this.channel = channel;
        this.connection = connection;
        this.connector = connector;
        this.currentConfig = this.config = configuration;
        this.adapter = configuration.getMessageAdapter();
        this.transactionType = configuration.getTransactionType();
        this.subscriptionFactory = subscriptionFactory;
        this.currentResolver = configuration.getDependencyResolver();
        this.identifier = counter.incrementAndGet();
        this.currentTransaction = new RabbitTransaction(
                this, this.transactionType);
        try {
            if (this.transactionType == RabbitTransactionType.Full)
                this.channel.txSelect();
            if (this.config.getChannelBuffer() <= 0 ||
                    this.config.getDispatchOnly())
                return;
            if (this.config.getTransactionType() ==
                    RabbitTransactionType.None)
                return;
            this.channel.basicQos(0, (int)this.config.getChannelBuffer(),
                    false);
        } catch (IOException ex) {
        }
    }

    @Override
    public void beginShutdown() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void receive(Action1<DeliveryContext> callback) {
        if (callback == null)
            throw new NullPointerException("callback must not be null");
        this.tryIt(() -> {
            this.subscription = this.subscriptionFactory.call();
            this.subscription.receive(this.config.getReceiveTimeout(),
                    (msg) -> this.receive(msg, callback));
        });
    }
    protected boolean receive(Delivery message,
            Action1<DeliveryContext> callback) {
        return false;
    }

    @Override
    public void send(ChannelEnvelope envelope) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean getActive() {
        return !this.disposed && !this.shutdown &&
                this.connector.getCurrentState() == ConnectionState.Open &&
                this.channel.getCloseReason() == null;
    }

    @Override
    public ChannelMessage getCurrentMessage() {
        return this.currentMessage;
    }

    @Override
    public DependencyResolver getCurrentResolver() {
        return this.currentResolver;
    }

    @Override
    public ChannelTransaction getCurrentTransaction() {
        return this.currentTransaction;
    }

    @Override
    public ChannelGroupConfiguration getCurrentConfiguration() {
        return this.currentConfig;
    }

    @Override
    public DispatchContext prepareDispatch(Object message, MessagingChannel channel) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void dispose() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    protected void tryIt(Action0 callback) {
        try {
            if (!this.suppressOperations)
                callback.run();
        } catch (Exception ex) {
            this.shutdownChannel(ex);
        }
    }

    private void shutdownChannel(Exception ex) {
        this.suppressOperations = true;
        this.dispose();
        this.connection.abort(1000);
        throw new ChannelConnectionException(ex.getMessage(), ex);
    }
}
