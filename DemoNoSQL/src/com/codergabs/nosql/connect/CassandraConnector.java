package com.codergabs.nosql.connect;

import java.util.logging.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.codergabs.config.ConfigReader;

public class CassandraConnector extends Connector {

	/** Cassandra Cluster. */
	private Cluster cluster;
	/** Cassandra Session. */
	private Session session;

	/**
	 * Connect to Cassandra Cluster specified by provided node IP address and
	 * port number.
	 * 
	 * @param node
	 *            Cluster node IP address.
	 * @param port
	 *            Port of cluster host.
	 */

	private static CassandraConnector instance = null;

	private CassandraConnector() {
		// Exists only to defeat instantiation.
	}

	public static CassandraConnector getInstance() {
		if (instance == null) {
			instance = new CassandraConnector();
		}
		return instance;
	}

	@Override
	protected void connect() {
		// to do change to config file read
		
		Logger.getLogger("CassandraConnector").info("Start getting Connection");
		
		ConfigReader cfg = new ConfigReader();
		final String node = cfg.getConfig("dbnode");
		final int port = Integer.valueOf(cfg.getConfig("dbport"));

		this.cluster = Cluster.builder().addContactPoint(node).withPort(port)
				.build();
		final Metadata metadata = cluster.getMetadata();
		Logger.getLogger("CassandraConnector").info("Connected to cluster:"+ metadata.getClusterName()+"\n" );
		for (final Host host : metadata.getAllHosts()) {
			Logger.getLogger("CassandraConnector").info("Datacenter: "+host.getDatacenter()+"; Host: "+host.getAddress()+"; Rack: "+host.getRack()+"\n");

		}
		session = cluster.connect();

		Logger.getLogger("CassandraConnector").info("End getting Connection");
	}

	@Override
	public void destroy() {
		
		Logger.getLogger("CassandraConnector").info("Start closing Connection");
		cluster.close();
		Logger.getLogger("CassandraConnector").info("End closing Connection");

	}

	/**
	 * Provide my Session.
	 * 
	 * @return My session.
	 */
	public Session getSession() {
		return this.session;
	}

}
