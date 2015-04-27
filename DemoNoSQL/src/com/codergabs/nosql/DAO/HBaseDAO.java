package com.codergabs.nosql.DAO;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class HBaseDAO extends NoSQLDAO {

	@Override
	public void insert(List<Object> colList, List<Object> valList,
			String tabName) {
		Logger.getLogger("HBaseDAO").info("Start insert");
		// TODO Auto-generated method stub

	}



	@Override
	public void deleteRow(String rowKey, String tabName) {
		// TODO Auto-generated method stub
		
	}



	@Override
	public List<List<Object>> query(List<Object> colList, String tabName) {
		// TODO Auto-generated method stub
		Logger.getLogger("HBaseDAO").info("Start query");
		
		
		return null;
	}



	@Override
	public void create(Map<Object, Object> colList, List<Object> primaryKey,
			String tabName) {
		Logger.getLogger("HBaseDAO").info("Start create");
		// TODO Auto-generated method stub
		
	}

}
