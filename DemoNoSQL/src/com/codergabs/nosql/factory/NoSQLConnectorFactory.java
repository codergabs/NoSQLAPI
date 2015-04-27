package com.codergabs.nosql.factory;

import com.codergabs.config.ConfigReader;
import com.codergabs.nosql.DAO.CassandraDAO;
import com.codergabs.nosql.DAO.NoSQLDAO;
import com.codergabs.nosql.connect.CassandraConnector;
import com.codergabs.nosql.connect.Connector;
import com.codergabs.nosql.connect.HBaseConnector;

public class NoSQLConnectorFactory {

	public static Connector getNoSQLConnector() {

		ConfigReader cfg = new ConfigReader();

		if ("Cassandra".equals(cfg.getConfig("dbtype"))) {
			Connector conn = CassandraConnector.getInstance();
			conn.init();
			return conn.getConnection();
		}
		if ("HBase".equals(cfg.getConfig("dbtype"))) {
			Connector conn = HBaseConnector.getInstance();
			conn.init();
			return conn.getConnection();
		}
		return null;
	}

}
