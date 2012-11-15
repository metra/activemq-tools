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

public class QueueSerializer implements MessageListener {

  private static final Logger logger = LoggerFactory.getLogger(QueueSerializer.class);

  private final ObjectOutputStream objectOutputStream;

  public static void main(String[] args) throws JMSException, IOException {

    if (args.length != 2) {
      logger.error("incorrect args given. usage: " + QueueSerializer.class.getSimpleName() + " <broker host:port> <queue name>");
    }

    String brokerUrl = args[0];
    String queueName = args[1];

    MessageListener queueSerializer = new QueueSerializer();
    DestinationManager.listenToQueue(brokerUrl, queueName, queueSerializer);
  }

  public QueueSerializer() throws IOException {
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
}
