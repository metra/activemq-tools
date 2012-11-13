package mq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

/**
 * User: ashnayder
 * Date: 10/31/12
 */
public class QueueManager {

  private static final Logger logger = LoggerFactory.getLogger(QueueManager.class);

  public static void listenTo(String brokerUrl, String queueName, MessageListener messageListener) throws JMSException {
    logger.info("creating listener");
    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
    Connection connection = connectionFactory.createConnection();
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination dest = session.createQueue(queueName);
    MessageConsumer consumer = session.createConsumer(dest);
    consumer.setMessageListener(messageListener);
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
