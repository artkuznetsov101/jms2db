package jms2db;

import java.beans.PropertyVetoException;
import java.sql.SQLException;

public interface DAOFactory extends AutoCloseable {

	@Override
	public void close();

	public MessageDAO getMessageDAO() throws SQLException;

	public static DAOFactory getPostgresFactory() throws PropertyVetoException, SQLException {
		return new DAOFactoryPostgres();
	}
}
