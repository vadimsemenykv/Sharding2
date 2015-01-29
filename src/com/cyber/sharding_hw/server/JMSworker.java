package com.cyber.sharding_hw.server;

import com.cyber.sharding_hw.server.jms_propertys_data.Direction;
import com.cyber.sharding_hw.server.jms_propertys_data.MsgTitle;
import com.cyber.sharding_hw.server.jms_propertys_data.PropertyName;
import com.sun.messaging.ConnectionConfiguration;
import com.sun.messaging.ConnectionFactory;
import javax.jms.*;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Vadim on 28.01.2015.
 */
public class JMSworker implements MessageListener {
    private JMSMessageProcessor processor;

    private ConnectionFactory factory;
    private TopicConnection connection;
    private TopicSession session;
    private TopicSubscriber consumer;
    private TopicPublisher sender;
    private AtomicBoolean stop = new AtomicBoolean(false);

    public JMSworker(String addressJMSServer, String jmsTopicName, String jmsBrockerUserName,
                     String jmsBrockerPass, JMSMessageProcessor processor) {
        this.processor = processor;
        try {
            StringBuilder imqAddressListBuilder = new StringBuilder();
            imqAddressListBuilder.append("mq://");
            imqAddressListBuilder.append(addressJMSServer);
            imqAddressListBuilder.append(",mq://");
            imqAddressListBuilder.append(addressJMSServer);
            String imqAddressList = imqAddressListBuilder.toString();

            factory = new com.sun.messaging.ConnectionFactory();
            factory.setProperty(ConnectionConfiguration.imqAddressList, imqAddressList);
            connection = factory.createTopicConnection(jmsBrockerUserName, jmsBrockerPass);
            connection.start();

            session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic ioQueue = session.createTopic(jmsTopicName);

            consumer = session.createSubscriber(ioQueue);
            consumer.setMessageListener(this);

            sender = session.createPublisher(ioQueue);
        } catch (JMSException e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            close();
        }
    }

    @Override
    public void onMessage(Message msg) {
        if (msg instanceof ObjectMessage){
            processor.processJMSMessage((ObjectMessage) msg);
        }
    }

    public void sendMessage(ObjectMessage msg){
        try {
            sender.send(msg);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String direction, String title){
        try {
            ObjectMessage message = session.createObjectMessage();
            message.setStringProperty(PropertyName.DIRECTION, direction);
            message.setStringProperty(PropertyName.MSG_TITLE, title);
            sender.send(message);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String direction, String title, String data){
        try {
            ObjectMessage message = session.createObjectMessage();
            message.setStringProperty(PropertyName.DIRECTION, direction);
            message.setStringProperty(PropertyName.MSG_TITLE, title);
            message.setStringProperty(PropertyName.MSG_DATA, data);
            sender.send(message);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(String direction, String title, Serializable obj){
        try {
            ObjectMessage message = session.createObjectMessage();
            message.setStringProperty(PropertyName.DIRECTION, direction);
            message.setStringProperty(PropertyName.MSG_TITLE, title);
            message.setObject(obj);
            sender.send(message);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public ObjectMessage getMessageBlank() {
        try {
            return session.createObjectMessage();
        } catch (JMSException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void stop() {
        stop.set(true);
        close();
    }

    private void close() {
        if (sender != null && stop.get()) {
            try {
                sender.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
        if (consumer != null && stop.get()) {
            try {
                consumer.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
        if (session != null && stop.get()) {
            try {
                session.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
        if (connection != null && stop.get()) {
            try {
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }
}
