package mq;

import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class Serializer implements MessageListener {

  private static final Logger logger = LoggerFactory.getLogger(Serializer.class);

  private final ObjectOutputStream objectOutputStream;

  public static void main(String[] args) throws JMSException, IOException {
    logger.info("starting");

    if (args.length != 3) {
      printUsage();
    }

    String brokerUrl = args[0];
    String destination = args[1];
    String destinationType = args[2];

    MessageListener serializer = new Serializer();
    if ("queue".equals(destinationType)) {
      DestinationManager.listenToQueue(brokerUrl, destination, serializer);
    } else if ("topic".equals(destinationType)) {
      DestinationManager.listenToTopic(brokerUrl, destination, serializer);
    } else {
      printUsage();
      return;
    }
  }

  public Serializer() throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream("queue.ser");
    objectOutputStream = new ObjectOutputStream(fileOutputStream);
  }

  public void onMessage(Message message) {
    ObjectMessage objectMessage = (ObjectMessage) message;

    SolrInputDocument solrInputDocument = null;
    try {
      solrInputDocument = (SolrInputDocument) objectMessage.getObject();
    } catch (JMSException e) {
      logger.error("", e);
      return;
    }

    logger.info("received message " + solrInputDocument);

    try {
      objectOutputStream.writeObject(solrInputDocument);
      objectOutputStream.flush();
    } catch (IOException e) {
      logger.error("", e);
    }
  }

  private static void printUsage() {
    System.out.println("usage: " + Serializer.class.getSimpleName() + " <broker host:port> <queue name> {queue|topic}");
  }
}
