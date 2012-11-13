package mq;


import ch.qos.logback.classic.BasicConfigurator;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class TopicListener implements MessageListener {

  private static final Logger logger = LoggerFactory.getLogger(TopicListener.class);

  public static void main(String[] args) throws JMSException {

//    if (args.length != 2) {
//      logger.error("incorrect args given. usage: " + Listener.class.getSimpleName() + " <broker host:port> <queue name>");
//    }

//    String brokerUrl = args[0];
//    String queueName = args[1];

    BasicConfigurator.configureDefaultContext();

    String brokerUrl = "tcp://devnn1.sv2.trulia.com:61616";
    String destName = "pearts_props.FOR_RENT.NEW.SLS";

    logger.info("starting");

    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
    Connection connection = connectionFactory.createConnection();
    connection.setClientID("this_is_a_unique_connection_id_54321");
    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Destination dest = session.createTopic(destName);
//    Destination dest = session.createQueue(queueName);
    MessageConsumer consumer = session.createDurableSubscriber((Topic) dest, "this_is_a_unique_id_12345");
//    MessageConsumer consumer = session.createConsumer(dest);
    MessageListener listener = new TopicListener();
    consumer.setMessageListener(listener);
    connection.start();
  }

  public void onMessage(Message message) {
    TextMessage textMessage = (TextMessage) message;
    logger.info("received message " + textMessage);
    try {
      logger.info("text: " + textMessage.getText());
    } catch (JMSException e) {
      logger.error("", e);
    }
  }
}
