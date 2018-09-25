package jms2db;

import java.beans.PropertyVetoException;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DAOFactoryPostgres implements DAOFactory {

	private ComboPooledDataSource datasource;

	public DAOFactoryPostgres() throws PropertyVetoException, SQLException {
		//
		System.setProperty("com.mchange.v2.log.MLog", "com.mchange.v2.log.FallbackMLog");
		System.setProperty("com.mchange.v2.log.FallbackMLog.DEFAULT_CUTOFF_LEVEL", "WARNING");

		datasource = new ComboPooledDataSource();
		datasource.setDriverClass(Config.TO.DATABASE_DRIVER);
		datasource.setJdbcUrl(Config.TO.DATABASE_URI);
		datasource.setUser(Config.TO.USERNAME);
		datasource.setPassword(Config.TO.PASSWORD);
	}

	@Override
	public void close() {
		datasource.close();
	}

	@Override
	public MessageDAO getMessageDAO() throws SQLException {
		return new MessageDAOPostgres(datasource.getConnection());
	}

}
