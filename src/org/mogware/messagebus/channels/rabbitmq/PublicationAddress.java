package org.mogware.messagebus.channels.rabbitmq;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PublicationAddress {
    private String exchangeName;
    private String exchangeType;
    private String routingKey;
    
    private static final Pattern PSEUDO_URI_PARSER = 
            Pattern.compile("^([^:]+)://([^/]*)/(.*)$");    
    
    public PublicationAddress(String exchangeType, String exchangeName,
            String routingKey) {
        this.exchangeType = exchangeType;
        this.exchangeName = exchangeName;
        this.routingKey = routingKey;
    }
    
    public String getExchangeName() {
        return this.exchangeName;
    }
    
    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }
    
    public String getExchangeType() {
        return this.exchangeType;
    }
    
    public void getExchangeType(String exchangeType) {
        this.exchangeType = exchangeType;
    }
    
    public String getRoutingKey() {
        return this.routingKey;
    }
    
    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }
    
    static PublicationAddress parse(String uriLikeString) {
        Matcher match = PSEUDO_URI_PARSER.matcher(uriLikeString);
        if (match.matches())
            return new PublicationAddress(match.group(1),
                    match.group(2), match.group(3));
        return null;
    }
    
    public String toString() { 
        return this.exchangeType + "://" + this.exchangeName + 
                "/" + this.routingKey; 
    } 
}
