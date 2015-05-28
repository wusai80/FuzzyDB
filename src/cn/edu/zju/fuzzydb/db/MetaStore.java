package cn.edu.zju.fuzzydb.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * meta store maintains the schema information
 * @author wusai
 *
 */
public class MetaStore {
	
	/**
	 * the singleton instance
	 */
	private static MetaStore instance = null;
	
	
	/**
	 * 
	 * @return the singleton instance
	 */
	public static MetaStore getInstance(){
		if(instance == null)
			instance = new MetaStore();
		return instance;
	}
	
	/**
	 * 
	 * @param table
	 * @return the column names of a table
	 * @throws SQLException 
	 */
	public String[] getColumns(String table) throws SQLException{
		
		Connection con = DBConnection.getConnection();
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("show columns from " + table);
		ArrayList<String> tname = new ArrayList<String>();
		while(rs.next()){
			tname.add(rs.getString(1));
		}
		return (String[])tname.toArray();
	}
	
	/**
	 * @param db
	 * @return all tables in a database
	 * @throws SQLException 
	 */
	public String[] getAllTables() throws SQLException{
		Connection con = DBConnection.getConnection();
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("show tables");
		
		ArrayList<String> tname = new ArrayList<String>();
		while(rs.next()){
			tname.add(rs.getString(1));
		}
		return (String[])tname.toArray();
	}
	
	/**
	 * 
	 * @param column
	 * @return check whether it is a dimension table
	 */
	public boolean isDimensionColumn(String column){
		return false;
	}
	
	/**
	 * 
	 * @param column
	 * @return check whether we need to compute aggregations for the column
	 */
	public boolean isQuantityColumn(String column){
		return false;
	}
	
}
