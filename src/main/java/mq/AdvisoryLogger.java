package mq;


import ch.qos.logback.classic.BasicConfigurator;
import org.apache.activemq.command.ActiveMQMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

public class AdvisoryLogger implements MessageListener {

  private static final Logger logger = LoggerFactory.getLogger(AdvisoryLogger.class);

  public static void main(String[] args) throws JMSException {

    if (args.length != 2) {
      logger.error("incorrect args given. usage: " + AdvisoryLogger.class.getSimpleName() + " <broker host:port> <topic name>");
    }

    String brokerUrl = args[0];
    String topicName = args[1];

    BasicConfigurator.configureDefaultContext();

    logger.info("starting");

    AdvisoryLogger topicLogger = new AdvisoryLogger();
    DestinationManager.listenToTopic(brokerUrl, topicName, topicLogger);
  }

  public void onMessage(Message message) {
    ActiveMQMessage activeMQMessage = (ActiveMQMessage) message;
    logger.info("received message " + activeMQMessage.getDataStructure());
    logger.info("text: " + activeMQMessage);
  }
}
