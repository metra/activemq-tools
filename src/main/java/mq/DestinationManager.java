package mq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;


public class DestinationManager {

  private static final Logger logger = LoggerFactory.getLogger(DestinationManager.class);

  public static void listenToQueue(String brokerUrl, String queueName, MessageListener messageListener) throws JMSException {
    logger.info("creating queue listener");
    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
    Connection connection = connectionFactory.createConnection();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination dest = session.createQueue(queueName);
    MessageConsumer consumer = session.createConsumer(dest);
    consumer.setMessageListener(messageListener);
    connection.start();
  }

  public static void listenToTopic(String brokerUrl, String topicName, MessageListener listener) throws JMSException {
    logger.info("creating topic listener");
    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
    Connection connection = connectionFactory.createConnection();
//    connection.setClientID("unique_client_id_123");
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic dest = session.createTopic(topicName);
    MessageConsumer consumer = session.createConsumer(dest);
//    MessageConsumer consumer = session.createDurableSubscriber(dest, "this_is_a_unique_id_12345");
    consumer.setMessageListener(listener);
    connection.start();
  }

  public static SessionAndProducer createSessionAndProducer(String brokerUrl, String queueName) throws JMSException {
    logger.info("creating producer");
    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
    Connection connection = connectionFactory.createConnection();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination dest = session.createQueue(queueName);
    MessageProducer producer = session.createProducer(dest);
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    connection.start();

    SessionAndProducer sessionAndProducer = new SessionAndProducer();
    sessionAndProducer.session = session;
    sessionAndProducer.producer = producer;

    return sessionAndProducer;
  }

  public static class SessionAndProducer {
    public Session session;
    public MessageProducer producer;
  }

}
