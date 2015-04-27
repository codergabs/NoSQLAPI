package com.codergabs.nosql.test;

import com.codergabs.nosql.connect.CassandraConnector;
import com.codergabs.nosql.connect.Connector;

public class TestConnection {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Connector conn = CassandraConnector.getInstance();
		conn.init();
		try {
			Thread.sleep(1000000000);
		} catch (InterruptedException e) {

		}

	}

}
