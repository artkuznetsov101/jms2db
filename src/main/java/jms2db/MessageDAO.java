package jms2db;

import java.sql.SQLException;

import javax.jms.JMSException;
import javax.jms.TextMessage;

public interface MessageDAO {

	long insert(TextMessage message) throws SQLException, JMSException;
}
