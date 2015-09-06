package org.mogware.messagebus.channels.rabbitmq;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import org.mogware.system.delegates.Action1;
import org.mogware.system.threading.TimeSpan;

public class RabbitWireup {
    private static final TimeSpan defaultTimeout = TimeSpan.fromSeconds(3);
    private final List<RabbitChannelGroupConfiguration> configurations =
            new LinkedList<>();
    private TimeSpan shutdownTimeout;
    private FailoverConnectionFactory connectionFactory;

    public List<RabbitChannelGroupConfiguration> getChannelGroups() {
        return new LinkedList<>(this.configurations);
    }

    public TimeSpan getShutdownTimeout() {
        return this.shutdownTimeout;
    }

    public void setShutdownTimeout(TimeSpan value) {
        this.shutdownTimeout = value;
    }

    public FailoverConnectionFactory getConnectionFactory() {
        return this.connectionFactory;
    }

    public void setConnectionFactory(FailoverConnectionFactory value) {
        this.connectionFactory = value;
    }

    public RabbitWireup withShutdownTimout(TimeSpan timeout) {
        if (timeout.compareTo(TimeSpan.zero) < 0)
            throw new IllegalArgumentException("timeout cannot be negative.");
        this.setShutdownTimeout(timeout);
        return this;
    }

    public RabbitWireup withConnectionFactory(
            FailoverConnectionFactory factory) {
        if (factory == null)
            throw new NullPointerException("factory must not be null");
        this.setConnectionFactory(factory);
        return this;
    }

    public RabbitWireup usingCertificateAuthentication() {
    /** Redo for Java
        this.getConnectionFactory().AuthMechanisms = new AuthMechanismFactory[]
        {
            new ExternalMechanismFactory(),
            new PlainMechanismFactory()
        };
    */
        return this;
    }

    public RabbitWireup addEndpoints(String addresses, boolean ordered) {
        this.getConnectionFactory().addEndpoints(addresses);
        if (!ordered)
            this.getConnectionFactory().randomizeEndpoints();
        return this;
    }

    public RabbitWireup addEndpoint(URI address, boolean ordered) {
        if (address == null)
            throw new NullPointerException("address must not be null");
        this.getConnectionFactory().addEndpoint(address);
        if (!ordered)
            this.getConnectionFactory().randomizeEndpoints();
        return this;
    }

    public RabbitWireup addChannelGroup(
            Action1<RabbitChannelGroupConfiguration> callback) {
        if (callback == null)
            throw new NullPointerException("callback must not be null");
        RabbitChannelGroupConfiguration config =
                new RabbitChannelGroupConfiguration();
        callback.run(config);
        this.configurations.add(config);
        return this;
    }

    public RabbitConnector build() {
        return new RabbitConnector(this.getConnectionFactory(),
                this.getShutdownTimeout(), this.configurations);
    }

    public RabbitWireup() {
        this.shutdownTimeout = defaultTimeout;
        this.connectionFactory = new FailoverConnectionFactory();
    }
}
