package jms2db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MessageDAOPostgres implements MessageDAO {
	private static final Logger log = LogManager.getLogger();

	private PreparedStatement stmt;

	public static final String SQL_TABLE = "message";

	public static final String SQL_CREATE = "CREATE TABLE IF NOT EXISTS " + SQL_TABLE
			+ " (id BIGSERIAL PRIMARY KEY, jms_type VARCHAR(255), jms_message_id VARCHAR(255), jms_correlation_id VARCHAR(255), jms_destination VARCHAR(255), jms_reply_to VARCHAR(255), jms_delivery_mode INTEGER, jms_redelivered BOOLEAN, jms_priority INTEGER, jms_expiration BIGINT, jms_timestamp TIMESTAMP, jms_data XML, created TIMESTAMP DEFAULT NOW())";

	public static final String SQL_INSERT = "INSERT INTO " + SQL_TABLE
			+ " (jms_type, jms_message_id, jms_correlation_id, jms_destination, jms_reply_to, jms_delivery_mode, jms_redelivered, jms_priority, jms_expiration, jms_timestamp, jms_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, XMLPARSE(DOCUMENT ?))";

	public MessageDAOPostgres(Connection connection) throws SQLException {
		connection.createStatement().executeUpdate(SQL_CREATE);

		stmt = connection.prepareStatement(SQL_INSERT, Statement.RETURN_GENERATED_KEYS);
	}

	@Override
	public long insert(TextMessage message) throws SQLException, JMSException {

		stmt.setString(1, message.getJMSType());
		stmt.setString(2, message.getJMSMessageID());
		stmt.setString(3, message.getJMSCorrelationID());
		stmt.setString(4, message.getJMSDestination() != null ? message.getJMSDestination().toString() : null);
		stmt.setString(5, message.getJMSReplyTo() != null ? message.getJMSReplyTo().toString() : null);
		stmt.setInt(6, message.getJMSDeliveryMode());
		stmt.setBoolean(7, message.getJMSRedelivered());
		stmt.setInt(8, message.getJMSPriority());
		stmt.setLong(9, message.getJMSExpiration());
		stmt.setTimestamp(10, new Timestamp(message.getJMSTimestamp()));
		stmt.setString(11, message.getText());

		stmt.executeUpdate();

		ResultSet rs = stmt.getGeneratedKeys();
		if (rs.next()) {
			log.debug("primary key for " + message.getJMSMessageID() + " is " + rs.getLong(1));
			return rs.getLong(1);
		}
		return -1;
	}
}
