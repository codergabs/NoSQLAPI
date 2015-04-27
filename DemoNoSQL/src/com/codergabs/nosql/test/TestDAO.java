package com.codergabs.nosql.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.codergabs.nosql.DAO.NoSQLDAO;
import com.codergabs.nosql.connect.Connector;
import com.codergabs.nosql.factory.NoSQLConnectorFactory;
import com.codergabs.nosql.factory.NoSQLDAOFactory;

public class TestDAO {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		Connector noSqlConn = NoSQLConnectorFactory.getNoSQLConnector();

		NoSQLDAO noSQLdb = NoSQLDAOFactory.getNoSQLDB();

		Map<Object,Object> colMap = new HashMap<Object, Object>();
		List<Object> primaryKeyList = new ArrayList<>();
		
		primaryKeyList.add("ROWKEY");
		
		String tabName = "demo.SAMPLE_TAB20";
		
		colMap.put("ROWKEY", "int");
		colMap.put("TASK_ID", "int");
		colMap.put("CREATED_BY", "varchar");
		colMap.put("STATUS", "int");
		colMap.put("ASSIGNED_TO", "varchar");
		colMap.put("PRIORITY", "int");
		
		noSQLdb.create(colMap, primaryKeyList, tabName);
		
		List<Object> colList = new ArrayList<>();
		List<Object> valList = new ArrayList<>();
		
		
		colList.add("ROWKEY");
		colList.add("TASK_ID");
		colList.add("CREATED_BY");
		colList.add("STATUS");
		colList.add("ASSIGNED_TO");
		colList.add("PRIORITY");
		
		valList.add(2);
		valList.add(6978);
		valList.add("admin");
		valList.add(0);
		valList.add("user");
		valList.add(1);
		noSQLdb.insert(colList, valList, tabName);

		List<List<Object>> obj = noSQLdb.query(colList, tabName);
		//noSQLdb.deleteRow("1", tabName);
		
		for (Object column:colList){
			System.out.println(column+" | ");
		}
		for (List<Object> row:obj){
			for (Object column:row){
				System.out.print(column+" | ");
			}
			System.out.print("\n");
		}
		
		noSqlConn.destroy();

	}

}
