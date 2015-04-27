package com.codergabs.nosql.connect;

import java.util.logging.Logger;

public class HBaseConnector extends Connector {

	private static HBaseConnector instance = null;

	private HBaseConnector() {
		// Exists only to defeat instantiation.
	}

	public static HBaseConnector getInstance() {
		if (instance == null) {
			instance = new HBaseConnector();
		}
		return instance;
	}
	
	@Override
	protected void connect() {
		Logger.getLogger("HBaseConnector").info("Start getting Connection");
		// TODO Auto-generated method stub

	}

	@Override
	public void destroy() {
		Logger.getLogger("HBaseConnector").info("Start closing Connection");
		// TODO Auto-generated method stub

	}

}
