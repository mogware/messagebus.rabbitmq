package org.mogware.messagebus.channels.rabbitmq;

import com.rabbitmq.client.QueueingConsumer.Delivery;
import org.mogware.system.Disposable;
import org.mogware.system.delegates.Func1;
import org.mogware.system.threading.TimeSpan;

public class RabbitSubscription implements Disposable {

    public void receive(TimeSpan timeout, Func1<Delivery, Boolean> callback) {
    }    
    
    @Override
    public void dispose() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
