package org.mogware.messagebus.channels.rabbitmq;

import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.mogware.messagebus.DispatchTable;
import static org.mogware.messagebus.channels.rabbitmq.ExtensionMethods.normalizeName;

public class RabbitDispatchTable implements DispatchTable {
    @Override
    public List<URI> getUri(Class messageType) {
        if (messageType == null)
            throw new NullPointerException("messageType must not be null");
        return Arrays.asList(URI.create("fanout://" +
                normalizeName(messageType.getName())));
    }

    @Override
    public void addSubscriber(URI subscriber, Class messageType,
            Date expiration) {
    }

    @Override
    public void addRecipient(URI recipient, Class messageType) {
    }

    @Override
    public void remove(URI address, Class messageType) {
    }
}
