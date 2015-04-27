package com.codergabs.nosql.factory;

import com.codergabs.config.ConfigReader;
import com.codergabs.nosql.DAO.CassandraDAO;
import com.codergabs.nosql.DAO.HBaseDAO;
import com.codergabs.nosql.DAO.NoSQLDAO;

public class NoSQLDAOFactory {

	public static NoSQLDAO getNoSQLDB() {

		ConfigReader cfg = new ConfigReader();

		if ("Cassandra".equals(cfg.getConfig("dbtype"))) {
			return new CassandraDAO();
		}
		if ("HBase".equals(cfg.getConfig("dbtype"))) {
			return new HBaseDAO();
		}
		return null;
	}
}
