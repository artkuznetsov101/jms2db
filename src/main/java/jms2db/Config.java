package jms2db;

import org.ini4j.Wini;

public class Config {

	public static String NAME = "jms2db.ini";

	public static class COMMON {
		static int TIMEOUT;
	}

	public static class FROM {
		static String BROKER_URI;
		static String DEST_TYPE;
		static String DEST_NAME;
		static String USERNAME;
		static String PASSWORD;
		static String CLIENTID;
		static String SUBSCRIPTION_NAME;
	}

	public static class TO {
		static String DATABASE_URI;
		static String DATABASE_DRIVER;
		static String USERNAME;
		static String PASSWORD;
	}

	public static void setConfig(Wini ini) {
		if ((Config.COMMON.TIMEOUT = ini.get("COMMON", "TIMEOUT", Integer.TYPE).intValue()) == 0)
			throw new IllegalArgumentException("COMMON->TIMEOUT parameter not specified in ini file. Exit");

		if ((Config.FROM.BROKER_URI = ini.get("FROM", "BROKER_URI")) == null)
			throw new IllegalArgumentException("FROM->BROKER_URI parameter not specified in ini file. Exit");
		if ((Config.FROM.DEST_TYPE = ini.get("FROM", "DEST_TYPE")) == null)
			throw new IllegalArgumentException("FROM->DEST_TYPE parameter not specified in ini file. Exit");
		if ((Config.FROM.DEST_NAME = ini.get("FROM", "DEST_NAME")) == null)
			throw new IllegalArgumentException("FROM->DEST_NAME parameter not specified in ini file. Exit");

		Config.FROM.USERNAME = ini.get("FROM", "USERNAME");
		Config.FROM.PASSWORD = ini.get("FROM", "PASSWORD");
		Config.FROM.CLIENTID = ini.get("FROM", "CLIENTID");
		Config.FROM.SUBSCRIPTION_NAME = ini.get("FROM", "SUBSCRIPTION_NAME");

		if ((Config.TO.DATABASE_URI = ini.get("TO", "DATABASE_URI")) == null)
			throw new IllegalArgumentException("TO->DATABASE_URI parameter not specified in ini file. Exit");
		if ((Config.TO.DATABASE_DRIVER = ini.get("TO", "DATABASE_DRIVER")) == null)
			throw new IllegalArgumentException("TO->DATABASE_DRIVER parameter not specified in ini file. Exit");

		Config.TO.USERNAME = ini.get("TO", "USERNAME");
		Config.TO.PASSWORD = ini.get("TO", "PASSWORD");
	}
}
