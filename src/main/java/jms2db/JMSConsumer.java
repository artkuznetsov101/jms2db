package jms2db;

import java.sql.SQLException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JMSConsumer extends Thread implements ExceptionListener { // , MessageListener
	private static final Logger log = LogManager.getLogger();

	Connection connection;
	Session session;
	Destination destination;
	MessageConsumer consumer;
	String destFrom;
	boolean isConnected = false;
	boolean isReceiving = false;
	MessageDAO db;

	public JMSConsumer(String destFrom, MessageDAO db) {
		this.destFrom = destFrom;
		this.db = db;
	}

	public void connect() {
		log.info("consumer -> [" + destFrom + "] connect");
		try {
			connection = JMSConnectionFactory.getActiveMQFactory(Config.FROM.BROKER_URI, Config.FROM.USERNAME, Config.FROM.PASSWORD).createConnection();
			if (!Config.FROM.CLIENTID.isEmpty())
				connection.setClientID(Config.FROM.CLIENTID);
			connection.setExceptionListener(this);
			session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
			if (Config.FROM.DEST_TYPE.equals("queue"))
				destination = session.createQueue(Config.FROM.DEST_NAME);
			else
				destination = session.createTopic(Config.FROM.DEST_NAME);
			if (Config.FROM.SUBSCRIPTION_NAME.isEmpty())
				consumer = session.createConsumer(destination);
			else
				consumer = session.createDurableSubscriber((Topic) destination, Config.FROM.SUBSCRIPTION_NAME);

			isConnected = true;
			startReceive();
		} catch (JMSException e) {
			log.error("consumer -> [" + destFrom + "] connect exception: " + e.getMessage());
		}
	}

	public void startReceive() {
		log.debug("consumer -> [" + destFrom + "] start receive");
		try {
//			if (consumer != null)
//				consumer.setMessageListener(this);
			isReceiving = true;
			if (connection != null)
				connection.start();
		} catch (JMSException e) {
			log.error("consumer -> [" + destFrom + "] start receive exception: " + e.getMessage());
		}
	}

	public void stopReceive() {
		log.debug("consumer -> [" + destFrom + "] stop receive");
		try {
			if (consumer != null)
				consumer.setMessageListener(null);
			if (connection != null)
				connection.stop();

			isReceiving = false;
		} catch (JMSException e) {
			log.error("consumer -> [" + destFrom + "] stop receive exception: " + e.getMessage());
		} finally {
			isReceiving = false;
		}
	}

	public void disconnect() {
		log.info("consumer -> [" + destFrom + "] disconnect");
		try {
			stopReceive();

			if (consumer != null) {
				consumer.close();
				consumer = null;
			}
			if (session != null) {
				session.close();
				session = null;
			}
			if (connection != null) {
				connection.close();
				connection = null;
			}
		} catch (JMSException e) {
			log.error("consumer -> [" + destFrom + "] disconnect exception: " + e.getMessage());
		}
	}

	@Override
	public void onException(JMSException e) {
		log.error("consumer -> [" + destFrom + "] onException: " + e.getMessage());

		disconnect();
		isConnected = false;
		Thread.currentThread().interrupt();
	}

//	@Override
//	public void onMessage(Message message) {
//		try {
//			log.debug("consumer -> [" + destFrom + "] get: " + message.getJMSMessageID());
//			exchange.put(message);
//			log.debug("consumer -> exchange put " + message.getJMSMessageID());
//
//			if (result.exchange(null)) {
//				session.commit();
//				log.info("consumer -> [" + destFrom + "] commit: " + message.getJMSMessageID());
//			} else {
//				exchange.clear();
//				session.rollback();
//				log.error("consumer -> [" + destFrom + "] rollback: " + message.getJMSMessageID());
//			}
//		} catch (Exception e) {
//			try {
//				exchange.clear();
//				session.rollback();
//				log.error("consumer -> [" + destFrom + "] rollback: " + e.getMessage());
//				try {
//					Thread.sleep(Config.COMMON.TIMEOUT);
//				} catch (InterruptedException e1) {
//				}
//			} catch (JMSException e1) {
//			}
//		}
//	}

	@Override
	public void run() {
		Thread.currentThread().setName("jms consumer");
		Message message = null;
		while (true) {
			try {
				if (this.isConnected == true) {
					if ((message = consumer.receive()) != null) {
						log.debug("consumer -> [" + destFrom + "] get: " + message.getJMSMessageID());

						if (message instanceof TextMessage) {
							db.insert((TextMessage) message);
						} else {
							log.error("consumer -> [" + destFrom + "] " + message.getJMSMessageID() + " is not a JMS TextMessage type!");
						}

						session.commit();
						log.info("consumer -> [" + destFrom + "] commit: " + message.getJMSCorrelationID());
					}
				} else {
					try {
						Thread.sleep(Config.COMMON.TIMEOUT);
					} catch (InterruptedException e) {
					}
				}
			} catch (JMSException | SQLException e) {
				try {
					session.rollback();
					log.error("consumer -> [" + destFrom + "] rollback");
					Thread.sleep(Config.COMMON.TIMEOUT);
				} catch (JMSException | InterruptedException e1) {
				}
			}
		}
	}
}
