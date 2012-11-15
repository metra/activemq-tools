package mq;


import ch.qos.logback.classic.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class TopicLogger implements MessageListener {

  private static final Logger logger = LoggerFactory.getLogger(TopicLogger.class);

  public static void main(String[] args) throws JMSException {

    if (args.length != 2) {
      logger.error("incorrect args given. usage: " + TopicLogger.class.getSimpleName() + " <broker host:port> <queue name>");
    }

    String brokerUrl = args[0];
    String topicName = args[1];

    BasicConfigurator.configureDefaultContext();

    logger.info("starting");

    TopicLogger topicLogger = new TopicLogger();
    DestinationManager.listenToTopic(brokerUrl, topicName, topicLogger);
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
