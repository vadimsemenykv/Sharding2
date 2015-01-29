package com.cyber.sharding_hw.server;

import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

/**
 * Created by Vadim on 28.01.2015.
 */
public interface JMSMessageProcessor {
    void processJMSMessage(ObjectMessage msg);
}
