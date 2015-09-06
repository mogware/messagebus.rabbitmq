package org.mogware.messagebus.channels.rabbitmq;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

public class FailoverConnectionFactory extends ConnectionFactory {
    private static final String defaultUserName = "guest";
    private static final String defaultPassword = defaultUserName;
    private static final int requestedHeartbeatInSeconds = 15;
    private static final URI defaultEndpoint =
            URI.create("amqp://guest:guest@localhost:5672/");
    private static final String endpointDelimiter = Pattern.quote("|");
    private static final String authenticationDelimiter = Pattern.quote(":");
    private final List<URI> brokers = new LinkedList<>();

    public FailoverConnectionFactory() {
    }

    public Iterable<URI> getEndpoints() {
        return this.brokers;
    }

    public FailoverConnectionFactory addEndpoints(String endpoints) {
        for (String endpoint: endpoints.split(endpointDelimiter)) {
            if (! endpoint.isEmpty())
                this.addEndpoint(URI.create(endpoint));
        }
        return this;
    }

    public FailoverConnectionFactory addEndpoints(URI[] endpoints) {
        for (URI endpoint: endpoints) {
            if (endpoint != null)
                this.addEndpoint(endpoint);
        }
        return this;
    }

    public FailoverConnectionFactory addEndpoints(Iterable<URI> endpoints) {
        for (URI endpoint: endpoints) {
            if (endpoint != null)
                this.addEndpoint(endpoint);
        }
        return this;
    }

    public boolean addEndpoint(URI endpoint) {
        if (endpoint == null)
            throw new NullPointerException("endpoint must not be null");
        if (this.brokers.contains(endpoint))
            return false;
        this.brokers.add(endpoint);
        if (!this.getUsername().equals(defaultUserName) ||
                !this.getPassword().equals(defaultPassword))
            return true;
        String userInfo = endpoint.getUserInfo();
        if (userInfo != null) {
            String[] authentication = userInfo.split(authenticationDelimiter);
            if (authentication.length > 0)
                this.setUsername(authentication[0]);
            if (authentication.length > 1)
                this.setPassword(authentication[1]);
        }
        return true;
    }

    public FailoverConnectionFactory randomizeEndpoints() {
        Random random = new Random();
        for (int i = this.brokers.size() - 1; i > 1; i--) {
            int next = random.nextInt(i + 1);
            URI item = this.brokers.get(next);
            this.brokers.add(next, this.brokers.get(i));
            this.brokers.add(i, item);
        }
        return this;
    }

    @Override
    public Connection newConnection() throws IOException {
        this.setRequestedHeartbeat(requestedHeartbeatInSeconds);
        this.setConnectionTimeout(10 * 1000); // 20 seconds
        Address[] endpoints = this.brokers.stream()
                .map((x) -> new Address(x.getHost(), x.getPort()))
                .toArray((size) -> new Address[size]);
        if (endpoints.length == 0)
            endpoints = new Address[] { new Address(
                    defaultEndpoint.getHost(),
                    defaultEndpoint.getPort())};
        return this.newConnection(endpoints);
    }
}
