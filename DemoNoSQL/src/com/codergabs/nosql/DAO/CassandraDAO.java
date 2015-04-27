package com.codergabs.nosql.DAO;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.codergabs.nosql.connect.CassandraConnector;
import com.codergabs.nosql.connect.Connector;
import java.util.logging.*;

;

public class CassandraDAO extends NoSQLDAO {

	@Override
	public void insert(List<Object> colList, List<Object> valList,
			String tabName) {
		// TODO Auto-generated method stub
		Logger.getLogger("CassandraDAO").info("Start Insert");

		Connector conn = CassandraConnector.getInstance();
		CassandraConnector client = (CassandraConnector) conn.getConnection();

		StringBuilder sql = new StringBuilder("INSERT INTO ");

		sql.append(tabName);
		sql.append(" (");
		for (ListIterator<Object> iter = colList.listIterator(); iter.hasNext();) {
			sql.append(iter.next());
			if (iter.hasNext())
				sql.append(" , ");
		}
		sql.append(" )");
		sql.append(" VALUES");
		sql.append(" (");
		for (ListIterator<Object> iter = valList.listIterator(); iter.hasNext();) {
			iter.next();
			sql.append("?");
			if (iter.hasNext())
				sql.append(" , ");
		}
		sql.append(" )");

		String sqlstr = sql.toString();

		com.datastax.driver.core.PreparedStatement statement = client
				.getSession().prepare(sqlstr);
		BoundStatement boundStatement = new BoundStatement(statement);
		client.getSession().execute(boundStatement.bind(valList.toArray()));

		Logger.getLogger("CassandraDAO").info("End Insert");
	}

	@Override
	public void deleteRow(String rowKey, String tabName) {

		Logger.getLogger("CassandraDAO").info("Start Delete");

		Connector conn = CassandraConnector.getInstance();
		CassandraConnector client = (CassandraConnector) conn.getConnection();

		StringBuilder sql = new StringBuilder("DELETE");
		sql.append(" FROM ");
		sql.append(tabName);
		sql.append(" WHERE ");
		sql.append(" ROWKEY =  ");
		sql.append(rowKey);

		String sqlstr = sql.toString();

		client.getSession().execute(sqlstr);

		Logger.getLogger("CassandraDAO").info("End Delete");

	}

	@Override
	public List<List<Object>> query(List<Object> colList, String tabName) {

		Logger.getLogger("CassandraDAO").info("Start Query");

		List<List<Object>> valObjArr = new ArrayList<List<Object>>();

		Connector conn = CassandraConnector.getInstance();
		CassandraConnector client = (CassandraConnector) conn.getConnection();

		StringBuilder sql = new StringBuilder("SELECT ");

		for (ListIterator<Object> iter = colList.listIterator(); iter.hasNext();) {
			sql.append(iter.next());
			if (iter.hasNext())
				sql.append(" , ");
		}

		sql.append(" FROM ");
		sql.append(tabName);

		String sqlstr = sql.toString();

		ResultSet rs = client.getSession().execute(sqlstr);

		Iterator<com.datastax.driver.core.Row> iter = rs.iterator();
		while (iter.hasNext()) {
			if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched())
				rs.fetchMoreResults();
			com.datastax.driver.core.Row row = iter.next();
			List<Object> valObj = new ArrayList<Object>();
			for (ListIterator<Object> iterVal = colList.listIterator(); iterVal
					.hasNext();) {

				String columnName = (String) iterVal.next();
				if (row.getColumnDefinitions().getType(columnName)
						.asJavaClass() == Integer.class)
					valObj.add(row.getInt(columnName));
				if (row.getColumnDefinitions().getType(columnName)
						.asJavaClass() == String.class)
					valObj.add(row.getString(columnName));
			}
			valObjArr.add(valObj);
		}

		Logger.getLogger("CassandraDAO").info("End Query");

		return valObjArr;

	}

	@Override
	public void create(Map<Object, Object> colList, List<Object> primaryKey,
			String tabName) {

		Logger.getLogger("CassandraDAO").info("Start create");

		Connector conn = CassandraConnector.getInstance();
		CassandraConnector client = (CassandraConnector) conn.getConnection();

		StringBuilder sql = new StringBuilder("CREATE COLUMNFAMILY ");
		sql.append(tabName);
		sql.append("(\n");

		Iterator entries = colList.entrySet().iterator();
		while (entries.hasNext()) {
			Entry thisEntry = (Entry) entries.next();
			String column = (String) thisEntry.getKey();
			String dataType = (String) thisEntry.getValue();

			sql.append(column + " " + dataType + ",\n");

		}

		sql.append(" PRIMARY KEY ( ");

		for (ListIterator<Object> iter = primaryKey.listIterator(); iter
				.hasNext();) {
			sql.append(iter.next());
			if (iter.hasNext())
				sql.append(" , ");
		}

		sql.append(" )\n");

		sql.append(")");

		String sqlstr = sql.toString();

		ResultSet rs = client.getSession().execute(sqlstr);

		Logger.getLogger("CassandraDAO").info("End create");
	}

}
