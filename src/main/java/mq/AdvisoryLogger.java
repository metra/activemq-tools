package mq;


import org.apache.activemq.command.ActiveMQMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

public class AdvisoryLogger implements MessageListener {

  private static final Logger logger = LoggerFactory.getLogger(AdvisoryLogger.class);

  public static void main(String[] args) throws JMSException {
    logger.info("starting");

    if (args.length != 1) {
      logger.error("incorrect args given. usage: " + AdvisoryLogger.class.getSimpleName() + " <broker host:port>");
    }

    String brokerUrl = args[0];

    AdvisoryLogger advisoryLogger = new AdvisoryLogger();
    DestinationManager.listenToTopic(brokerUrl, "ActiveMQ.Advisory.>", advisoryLogger);
  }

  public void onMessage(Message message) {
    ActiveMQMessage activeMQMessage = (ActiveMQMessage) message;
    logger.info("advisory message: " + activeMQMessage);
    logger.info("data structure: " + activeMQMessage.getDataStructure());
  }
}
