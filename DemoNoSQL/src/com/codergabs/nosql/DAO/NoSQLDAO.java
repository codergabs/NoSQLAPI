package com.codergabs.nosql.DAO;

import java.util.List;
import java.util.Map;

public abstract class NoSQLDAO {

	public abstract void create(Map<Object,Object> colList, List<Object> primaryKey,
			String tabName);
	
	public abstract void insert(List<Object> colList, List<Object> valList,
			String tabName);

	// protected abstract void update(List<Object> colList, List<Object>
	// valList, String tabName);

	public abstract void deleteRow(String rowKey,  String tabName);

	public abstract List<List<Object>> query(List<Object> colList,
			String tabName);

}
