package com.codergabs.nosql.connect;

public abstract class Connector {

	public void init() {
		connect();
	}

	protected abstract void connect();

	public abstract void destroy();

	public Connector getConnection() {
		return this;
	}

}
