package jms2db;

import java.beans.PropertyVetoException;
import java.sql.SQLException;

public class JMSThread implements Runnable {
	JMSConsumer consumer;
	MessageDAO messageDAO;
	boolean isClosed = false;

	public JMSThread() throws PropertyVetoException, SQLException {
		messageDAO = DAOFactory.getPostgresFactory().getMessageDAO();

		consumer = new JMSConsumer(Config.FROM.DEST_NAME, messageDAO);
		consumer.start();
	}

	@Override
	public void run() {
		while (!isClosed) {
			if (!consumer.isConnected) {
				consumer.connect();
			}
			try {
				Thread.sleep(Config.COMMON.TIMEOUT);
			} catch (InterruptedException e) {
			}
		}
	}

	public void stop() {
		consumer.stopReceive();
	}

	public void start() {
		consumer.startReceive();
	}

	public void close() {
		isClosed = true;
		consumer.disconnect();
		try {
			DAOFactory.getPostgresFactory().close();
		} catch (PropertyVetoException | SQLException e) {
		}
	}
}
