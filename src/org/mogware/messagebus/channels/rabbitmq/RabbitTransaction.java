package org.mogware.messagebus.channels.rabbitmq;

import org.mogware.messagebus.ChannelTransaction;
import org.mogware.system.delegates.Action1;

public class RabbitTransaction implements ChannelTransaction {
    private final RabbitChannel channel;
    private final RabbitTransactionType transactionType;

    public RabbitTransaction(RabbitChannel channel,
            RabbitTransactionType transactionType) {
        this.channel = channel;
        this.transactionType = transactionType;
    }

    @Override
    public boolean getFinished() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void register(Action1 callback) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void dispose() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
