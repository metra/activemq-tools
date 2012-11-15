package mq;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class QueueLogger implements MessageListener {

  private static final Logger logger = LoggerFactory.getLogger(QueueLogger.class);

  public static void main(String[] args) throws JMSException {
    if (args.length != 2) {
      logger.error("incorrect args given. usage: " + QueueLogger.class.getSimpleName() + " <broker host:port> <queue name>");
    }

    String brokerUrl = args[0];
    String queueName = args[1];

    MessageListener queueLogger = new QueueLogger();
    DestinationManager.listenToQueue(brokerUrl, queueName, queueLogger);
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
