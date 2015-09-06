package org.mogware.messagebus.channels.rabbitmq;

import com.rabbitmq.client.AMQP.Queue;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mogware.messagebus.ChannelGroupConfiguration;
import org.mogware.messagebus.ChannelMessage;
import org.mogware.messagebus.ChannelMessageBuilder;
import org.mogware.messagebus.DefaultChannelMessageBuilder;
import org.mogware.messagebus.DependencyResolver;
import org.mogware.messagebus.DispatchTable;
import static org.mogware.messagebus.channels.rabbitmq.ExtensionMethods.normalizeName;
import static org.mogware.messagebus.channels.rabbitmq.ExtensionMethods.toExchangeAddress;
import org.mogware.messagebus.serialization.Serializer;
import org.mogware.system.threading.TimeSpan;

public class RabbitChannelGroupConfiguration
        implements ChannelGroupConfiguration {
    private static final String defaultGroupName = "unnamed-group";
    private static final String defaultAppId = "rabbit-endpoint";

    private PublicationAddress poisonMessageExchange =
            new PublicationAddress("fanout", "poison-messages", "");
    private PublicationAddress deadLetterExchange =
            new PublicationAddress("fanout", "dead-letters", "");
    private PublicationAddress unhandledMessageExchange =
            new PublicationAddress("fanout", "unhandled-messages", "");
    private PublicationAddress unroutableMessageExchange =
            new PublicationAddress("fanout", "unroutable-messages", "");
    private final List<Class> messageTypes = new ArrayList<>();

    private String groupName;
    private String inputQueue;
    private String applicationId;
    private TimeSpan receiveTimeout;
    private RabbitTransactionType transactionType;
    private RabbitMessageAdapter messageAdapter;
    private ChannelMessageBuilder messageBuilder;
    private DependencyResolver dependencyResolver;
    private DispatchTable dispatchTable;
    private Serializer serializer;
    private int minWorkers;
    private int maxWorkers;
    private int maxDispatchBuffer;
    private int maxAttempts;

    private boolean synchronous;
    private boolean dispatchOnly;
    private boolean skipDeclarations;
    private boolean clustered;
    private boolean randomInputQueue;
    private boolean durableQueue;
    private boolean exclusiveQueue;
    private boolean autoDelete;
    private boolean purgeOnStartup;
    private boolean returnAddressSpecified;
    private long channelBuffer;
    private URI returnAddress;

    public RabbitChannelGroupConfiguration() {
        this.groupName = defaultGroupName;
        this.applicationId = defaultAppId;
        this.receiveTimeout = TimeSpan.fromMilliseconds(1500);
        this.minWorkers = this.maxWorkers = 1;
        this.channelBuffer = 1024L;
        this.maxAttempts = 3;
        this.maxDispatchBuffer = Integer.MAX_VALUE;
        this.transactionType = RabbitTransactionType.Full;

        this.messageAdapter = new RabbitMessageAdapter(this);
        this.dependencyResolver = null;
        this.synchronous = false;
        this.dispatchOnly = true;
        this.durableQueue = true;

        this.messageBuilder = new DefaultChannelMessageBuilder();
        this.dispatchTable = new RabbitDispatchTable();
    }

    public void configureChannel(Channel channel) {
        if (this.skipDeclarations)
            return;
        this.declareSystemExchange(channel, this.poisonMessageExchange);
        this.declareSystemExchange(channel, this.deadLetterExchange);
        this.declareSystemExchange(channel, this.unhandledMessageExchange);
        this.declareSystemExchange(channel, this.unroutableMessageExchange);
        this.declareExchanges(channel);
        this.declareQueue(channel);
        this.bindQueue(channel);
    }

    protected void declareSystemExchange(Channel channel,
            PublicationAddress address) {
        if (this.getDispatchOnly() || address == null)
            return;
        try {
            channel.exchangeDeclare(address.getExchangeName(),
                    address.getExchangeType(), true, false, null);
            channel.queueDeclare(address.getExchangeName(),
                    true, false, false, null);
            channel.queueBind(address.getExchangeName(),
                    address.getExchangeName(), null);
        } catch (IOException ex) {
        }
    }

    protected void declareExchanges(Channel channel) {
        this.messageTypes.stream().forEach((type) -> {
            try {
                channel.exchangeDeclare(normalizeName(type.getName()),
                        "fanout", true, false, null);
            } catch (IOException ex) {
            }
        });
    }

    protected void declareQueue(Channel channel) {
        if (this.getDispatchOnly())
            return;
        Map<String, Object> declarationArgs = new HashMap<>();
        if (this.deadLetterExchange != null)
            declarationArgs.put("x-dead-letter-exchange",
                    this.deadLetterExchange.getExchangeName());
        if (this.clustered)
            declarationArgs.put("x-ha-policy", "all");
        String inputQueue = this.inputQueue;
        if (randomInputQueue)
            inputQueue = "";
        try {
            Queue.DeclareOk declaration = channel.queueDeclare(inputQueue,
                    this.durableQueue, this.exclusiveQueue,
                    this.autoDelete, declarationArgs);
            if (declaration != null)
                this.inputQueue = declaration.getQueue();
            if (!this.returnAddressSpecified)
                this.returnAddress = URI.create("direct://default/" +
                        this.inputQueue);
            if (this.purgeOnStartup)
                channel.queuePurge(this.inputQueue);
            channel.basicQos(0, (int) this.channelBuffer, false);
        } catch (IOException ex) {
        }
    }

    protected void bindQueue(Channel channel) {
        if (this.getDispatchOnly())
            return;
        this.messageTypes.stream().forEach((type) -> {
            try {
                channel.queueBind(this.inputQueue,
                    normalizeName(type.getName()), "", null);
            } catch (IOException ex) {
            }
        });
    }

    public String lookupRoutingKey(ChannelMessage message) {
        return normalizeName(
                message.getMessages().get(0).getClass().getName());
    }

    public String getInputQueue() {
        return this.inputQueue;
    }

    public RabbitTransactionType getTransactionType() {
        return this.transactionType;
    }

    public RabbitMessageAdapter getMessageAdapter() {
        return this.messageAdapter;
    }

    public String getApplicationId() {
        return this.applicationId;
    }

    public Serializer getSerializer() {
        return this.serializer;
    }

    public long getChannelBuffer() {
        return this.channelBuffer;
    }

    @Override
    public String getGroupName() {
        return this.groupName;
    }

    @Override
    public boolean getSynchronous() {
        return this.synchronous;
    }

    @Override
    public boolean getDispatchOnly() {
        return this.dispatchOnly;
    }

    @Override
    public int getMaxDispatchBuffer() {
        return this.maxDispatchBuffer;
    }

    @Override
    public int getMinWorkers() {
        return this.minWorkers;
    }

    @Override
    public int getMaxWorkers() {
        return this.maxWorkers;
    }

    @Override
    public URI getReturnAddress() {
        return this.returnAddress;
    }

    @Override
    public ChannelMessageBuilder getMessageBuilder() {
        return this.messageBuilder;
    }

    @Override
    public TimeSpan getReceiveTimeout() {
        return this.receiveTimeout;
    }

    @Override
    public DependencyResolver getDependencyResolver() {
        return this.dependencyResolver;
    }

    @Override
    public DispatchTable getDispatchTable() {
        return this.dispatchTable;
    }

    public RabbitChannelGroupConfiguration withGroupName(String name) {
        if (name == null)
            throw new NullPointerException("name must not be null");
        this.groupName = name;
        return this;
    }

    public RabbitChannelGroupConfiguration withReceiveTimeout(TimeSpan timeout) {
        if (timeout.compareTo(TimeSpan.zero) < 0)
            throw new IllegalArgumentException("Timeout must be positive");
        this.receiveTimeout = timeout;
        return this;
    }

    public RabbitChannelGroupConfiguration withWorkers(int min, int max) {
        if (min <= 0)
            throw new IllegalArgumentException("At least one worker must " +
                    "be specified.");
        if (min > max)
            throw new IllegalArgumentException("The maximum workers specified" +
                    "must be at least the same as the minimum specified.");
        this.minWorkers = min;
        this.maxWorkers = max;
        return this;
    }

    public RabbitChannelGroupConfiguration withInputQueue(String name,
            boolean clustered) {
        if (name == null)
            throw new NullPointerException("name must not be null");
        name = normalizeName(name);
        if (!this.returnAddressSpecified)
            this.returnAddress = URI.create("direct://default/" + name);
        this.inputQueue = name;
        this.autoDelete = this.autoDelete || this.inputQueue.isEmpty();
        this.dispatchOnly = false;
        this.groupName = this.groupName.equals(defaultGroupName) ?
                name : this.groupName;
        this.clustered = clustered;
        return this;
    }

    public RabbitChannelGroupConfiguration withRandomInputQueue() {
        this.randomInputQueue = true;
        return this.withInputQueue("", false);
    }

    public RabbitChannelGroupConfiguration withExclusiveReceive() {
        this.inputQueue = this.inputQueue != null ? this.inputQueue : "";
        this.dispatchOnly = false;
        this.exclusiveQueue = this.autoDelete = true;
        return this;
    }

    public RabbitChannelGroupConfiguration withAutoDeleteQueue() {
        this.inputQueue = this.inputQueue != null ? this.inputQueue : "";
        this.autoDelete = true;
        this.dispatchOnly = false;
        return this;
    }

    public RabbitChannelGroupConfiguration withTransientQueue() {
        this.inputQueue = this.inputQueue != null ? this.inputQueue : "";
        this.dispatchOnly = false;
        this.durableQueue = false;
        return this;
    }

    public RabbitChannelGroupConfiguration withCleanQueue() {
        this.purgeOnStartup = true;
        return this;
    }

    public RabbitChannelGroupConfiguration withSynchronousOperation() {
        this.synchronous = true;
        return this;
    }

    public RabbitChannelGroupConfiguration withDispatchOnly() {
        this.dispatchOnly = true;
        this.inputQueue = null;
        return this;
    }

    public RabbitChannelGroupConfiguration withMaxDispatchBuffer(
            int maxMessageCount) {
        if (maxMessageCount <= 0)
            throw new IllegalArgumentException("A non-negative message " +
                    "count is required.");
        this.maxDispatchBuffer = maxMessageCount;
        return this;
    }

    public RabbitChannelGroupConfiguration withTransaction(
            RabbitTransactionType transaction) {
        this.transactionType = transaction;
        return this;
    }

    public RabbitChannelGroupConfiguration withChannelBuffer(
            long maxMessageBuffer) {
        if (maxMessageBuffer < 0)
            throw new IllegalArgumentException("A non-negative buffer size " +
                    "is required.");
        if (maxMessageBuffer > Integer.MAX_VALUE)
            maxMessageBuffer = Integer.MAX_VALUE;
        this.channelBuffer = maxMessageBuffer;
        return this;
    }

    public RabbitChannelGroupConfiguration withChannelMessageBuilder(
            ChannelMessageBuilder builder) {
        if (builder == null)
            throw new NullPointerException("builder must not be null");
        this.messageBuilder = builder;
        return this;
    }

    public RabbitChannelGroupConfiguration withReturnAddress(URI address) {
        if (address == null)
            throw new NullPointerException("address must not be null");
        this.returnAddressSpecified = true;
        this.returnAddress = address;
        return this;
    }

    public RabbitChannelGroupConfiguration withPoisonMessageExchange(
            String exchange) {
        if (exchange == null)
            throw new NullPointerException("exchange must not be null");
        this.poisonMessageExchange = toExchangeAddress(exchange);
        return this;
    }

    public RabbitChannelGroupConfiguration withDeadLetterExchange(
            String exchange) {
        if (exchange == null)
            throw new NullPointerException("exchange must not be null");
        this.deadLetterExchange = toExchangeAddress(exchange);
        return this;
    }

    public RabbitChannelGroupConfiguration withUnhandledMessageExchange(
            String exchange) {
        if (exchange == null)
            throw new NullPointerException("exchange must not be null");
        this.unhandledMessageExchange = toExchangeAddress(exchange);
        return this;
    }

    public RabbitChannelGroupConfiguration withUnroutableMessageExchange(
            String exchange) {
        if (exchange == null)
            throw new NullPointerException("exchange must not be null");
        this.unroutableMessageExchange = toExchangeAddress(exchange);
        return this;
    }

    public RabbitChannelGroupConfiguration withMaxAttempts(int attempts) {
        if (attempts <= 0)
            throw new IllegalArgumentException("The maximum number of " +
                    "attempts must be positive");
        this.maxAttempts = attempts;
        return this;
    }

    public RabbitChannelGroupConfiguration withApplicationId(
            String identifier) {
        if (identifier == null)
            throw new NullPointerException("identifier must not be null");
        this.applicationId = identifier.trim();
        return this;
    }

    public RabbitChannelGroupConfiguration withSerializer(
            Serializer serializer) {
        if (serializer == null)
            throw new NullPointerException("serializer must not be null");
        this.serializer = serializer;
        return this;
    }

    public RabbitChannelGroupConfiguration withMessageTypes(
            Iterable<Class> handledTypes) {
        if (handledTypes == null)
            throw new NullPointerException("handledTypes must not be null");
        for (Class type: handledTypes)
            this.messageTypes.add(type);
        return this;
    }

    public RabbitChannelGroupConfiguration withDependencyResolver(
            DependencyResolver resolver) {
        if (resolver == null)
            throw new NullPointerException("resolver must not be null");
        this.dependencyResolver = resolver;
        return this;
    }

    public RabbitChannelGroupConfiguration withDispatchTable(
            DispatchTable table) {
        if (table == null)
            throw new NullPointerException("table must not be null");
        this.dispatchTable = table;
        return this;
    }
}
