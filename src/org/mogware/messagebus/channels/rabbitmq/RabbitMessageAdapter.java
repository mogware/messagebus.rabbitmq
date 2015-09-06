package org.mogware.messagebus.channels.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mogware.messagebus.ChannelMessage;
import org.mogware.messagebus.DeadLetterException;
import org.mogware.messagebus.PoisonMessageException;
import static org.mogware.messagebus.channels.rabbitmq.ExtensionMethods.getType;
import static org.mogware.messagebus.channels.rabbitmq.ExtensionMethods.toDateTime;
import static org.mogware.messagebus.channels.rabbitmq.ExtensionMethods.toEmpty;
import static org.mogware.messagebus.channels.rabbitmq.ExtensionMethods.toGuid;
import static org.mogware.messagebus.channels.rabbitmq.ExtensionMethods.toUri;
import org.mogware.messagebus.serialization.Serializer;
import org.mogware.messagebus.serialization.SerializationExtensions;

public class RabbitMessageAdapter {
    private final RabbitChannelGroupConfiguration configuration;

    public RabbitMessageAdapter(RabbitChannelGroupConfiguration config) {
        this.configuration = config;
    }

    public ChannelMessage build(Delivery message) {
        if (message == null)
            throw new NullPointerException("message must not be null");
        try {
            ChannelMessage result = this.translate(message);
            this.appendHeaders(result, message.getProperties());
            return result;
        } catch (DeadLetterException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new PoisonMessageException(ex.getMessage(), ex);
        }
    }

    protected ChannelMessage translate(Delivery message) {
        BasicProperties properties = message.getProperties();
        LocalDateTime despatched = toDateTime(properties.getTimestamp());
        LocalDateTime expiration = toDateTime(
                properties.getExpiration(), despatched);
        expiration = expiration.isEqual(LocalDateTime.MIN) ?
                LocalDateTime.MAX : expiration;
        if (!expiration.isAfter(LocalDateTime.now()))
            throw new DeadLetterException(expiration);
        List<Object> payload = this.deserialize(
                message.getBody(),
                properties.getType(),
                properties.getContentType(),
                properties.getContentEncoding());
        ChannelMessage channelMessage = new ChannelMessage(
                toGuid(properties.getMessageId()),
                toGuid(properties.getCorrelationId()),
                toUri(properties.getReplyTo()),
                new HashMap<>(), payload);
        channelMessage.setDispatched(despatched);
        channelMessage.setExpiration(expiration);
        channelMessage.setPersistent(properties.getDeliveryMode() == 2);
        return channelMessage;
    }

    private List<Object> deserialize(byte[] body, String type, String format,
            String encoding) {
        Object deserialized = SerializationExtensions.deserialize(
                this.configuration.getSerializer(),
                body, getType(type), format, encoding);
        return (List<Object>) deserialized;
    }

    protected void appendHeaders(ChannelMessage message,
            BasicProperties properties) {
        Map<String, String> headers = message.getHeaders();
        headers.put("x-rabbit-appId", properties.getAppId());
        headers.put("x-rabbit-clusterId", properties.getClusterId());
        headers.put("x-rabbit-userId", properties.getUserId());
        headers.put("x-rabbit-type", properties.getType());
        headers.put("x-rabbit-priority", properties.getPriority().toString());
        final Charset encoding = Charset.forName("UTF-8");
        properties.getHeaders().keySet().stream().forEach((key) -> {
            Object value = properties.getHeaders().get(key);
            if (value instanceof Integer)
                headers.put(key, ((Integer)value).toString());
            else
                headers.put(key, new String(((byte[]) value), encoding));
        });
    }

    public Delivery build(ChannelMessage message, BasicProperties properties) {
        if (message == null)
            throw new NullPointerException("message must not be null");
        if (properties == null)
            throw new NullPointerException("properties must not be null");
        try {
            return this.translate(message, properties);
        } catch (Exception ex) {
            throw new PoisonMessageException(ex.getMessage(), ex);
        }
    }

    protected Delivery translate(ChannelMessage message,
            BasicProperties properties) {
        Serializer serializer = this.configuration.getSerializer();
        Builder propertiesBuilder = properties.builder();
        propertiesBuilder.messageId(toEmpty(message.getMessageId()));
        propertiesBuilder.correlationId(toEmpty(message.getCorrelationId()));
        propertiesBuilder.appId(this.configuration.getApplicationId());
        String contentEncoding = serializer.getContentEncoding();
        propertiesBuilder.contentEncoding(contentEncoding != null ?
                contentEncoding : "");
        String contentType = serializer.getContentFormat();
        if (contentType != null && !contentType.isEmpty())
            contentType = "application/vnd.nmb.rabbit-msg +" + contentType;
        propertiesBuilder.contentType(contentType);

        return new Delivery(
                null,
                propertiesBuilder.build(),
                null
        );
    }
}
