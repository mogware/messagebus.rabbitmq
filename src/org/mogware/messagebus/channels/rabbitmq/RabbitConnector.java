package org.mogware.messagebus.channels.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.mogware.messagebus.ChannelConnectionException;
import org.mogware.messagebus.ChannelConnector;
import org.mogware.messagebus.ChannelGroupConfiguration;
import org.mogware.messagebus.ConnectionState;
import org.mogware.messagebus.MessagingChannel;
import org.mogware.system.threading.TimeSpan;

public class RabbitConnector implements ChannelConnector {
    private final Map<String, RabbitChannelGroupConfiguration> configuration;
    private final ConnectionFactory factory;
    private final int shutdownTimeout;
    private ConnectionState currentState;
    private Connection connection;
    private boolean disposed;
    

    public RabbitConnector(ConnectionFactory factory, TimeSpan shutdownTimeout,
            Iterable<RabbitChannelGroupConfiguration> config) {
        if (factory == null)
            throw new NullPointerException("factory must not be null"); 
        if (config == null)
            throw new NullPointerException("configuration must not be null");            
        this.factory = factory;
        this.shutdownTimeout = (int) shutdownTimeout.getTotalMilliseconds();
        this.configuration = StreamSupport.stream(config.spliterator(), false)
                .filter((x) -> x != null)
                .filter((x) -> !x.getGroupName().isEmpty())
                .collect(Collectors.toMap(x -> x.getGroupName(), x -> x));
        if (this.configuration.isEmpty())
            throw new IllegalArgumentException("No configurations provided.");
    }

    @Override
    public ConnectionState getCurrentState() {
        return this.currentState;
    }

    public void setCurrentState(ConnectionState connectionState) {
        this.currentState = connectionState;
    }
    
    @Override
    public Iterable<ChannelGroupConfiguration> getChannelGroups() {
        Collection<? extends ChannelGroupConfiguration> channelgroups = 
                this.configuration.values();
        return (Iterable<ChannelGroupConfiguration>) channelgroups;
    }

    @Override
    public MessagingChannel connect(String channelGroup) {
        RabbitChannelGroupConfiguration config =
                this.getChannelGroupConfiguration(channelGroup);
        synchronized (this) {
            return this.establishChannel(config);
        }
    }

    protected RabbitChannelGroupConfiguration getChannelGroupConfiguration(
            String channelGroup) {
        return null;
    }   
    
    protected MessagingChannel establishChannel(
            RabbitChannelGroupConfiguration config) {    
        return null;
    }
    
    protected Channel establishChannel() {
        Channel channel = null;
        try {
            if (this.connection != null)
                return this.connection.createChannel();
            this.currentState = ConnectionState.Opening;            
            this.connection = this.factory.newConnection();
            final RabbitConnector self = this;
            this.connection.addShutdownListener((cause) -> {
                self.close(null, ConnectionState.Closed, cause);
            });
            this.currentState = ConnectionState.Open;
            channel = this.connection.createChannel();
            this.initializeConfigurations(channel);
        } catch (IOException ex) {
            this.close(channel, ConnectionState.Disconnected, ex);
        }
        return channel;        
    }
    
    protected void initializeConfigurations(Channel channel) {
        for (RabbitChannelGroupConfiguration cfg: this.configuration.values())
            cfg.configureChannel(channel);
    }
    
    @Override
    public void dispose() {
        if (this.disposed)
            return;
        synchronized (this) {
            this.disposed = true;
            this.close(null, ConnectionState.Closed, null);
        }
    }

    protected void close(Channel channel, ConnectionState state, Exception ex) {
        this.currentState = ConnectionState.Closing;
	if (channel != null) 
            try { channel.abort(); } catch (IOException e) {}
        this.tryAbortConnection();
        this.connection = null;
        this.currentState = state;
        if (ex != null)
            throw new ChannelConnectionException(ex.getMessage(), ex);
    } 
    
    private void tryAbortConnection() {
        Connection currentConnection = this.connection;
	if (currentConnection == null)
            return;
        currentConnection.abort(this.shutdownTimeout);
    }
}
