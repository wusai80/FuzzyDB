package cn.edu.zju.fuzzydb.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import cn.edu.zju.fuzzydb.file.GroupPredicate;
import cn.edu.zju.fuzzydb.file.Property;

/**
 * a connection to the embbeded H2 database
 * @author wusai
 *
 */
public class DBConnection {
	
	private static Connection conn;
	
	public static Connection getConnection(){
		if(conn==null){
			try {
				Class.forName("org.h2.Driver");
				conn = DriverManager.
				        getConnection("jdbc:h2:./"+Property.dbName, "sa", "");
				init();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	   
			
		}
		return conn;
	}
	
	public static void close(){
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void init() throws SQLException{
		String sql = "create table " + Property.configTable + " if not exists " +
	                 "(groupid int,  grouppredicate VARCHAR(1024), groupTable VARCHAR(100), selectpredicate TEXT, sampleSize int)";
		
		Statement stmt = conn.createStatement();
		stmt.executeUpdate(sql);
		
		sql = "create table " + Property.progressTable + " if not exists " + 
				"(tablename VARCHAR(100) primary key, start long, end long)";
		stmt.executeUpdate(sql);
		stmt.close();
	}
	
	
	
	public static void createNewSampleTable(GroupPredicate gp){
		
		//first check whether the table with the same name exists
		ArrayList<String> tnames = new ArrayList<String>();
		String testTableName = Integer.toString(gp.getIndexCode());
		String sql = "show tables";
		Statement stmt;
		try {
			stmt = conn.createStatement();
			ResultSet rs =stmt.executeQuery(sql);	
			while(rs.next()){
				tnames.add(rs.getString(1));
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//update the table name;
		while(tnames.contains(testTableName)){
			testTableName += "x";
		}
		gp.setTableName(testTableName);

		sql = "insert into " + Property.configTable +" values (" +
				gp.getIndexCode() + ",'" + gp.toString() + "','" + gp.getTable() + "','')";
	
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			//create an empty table by copying the schema information from base table
			sql = "create table if not exists " + gp.getTable()  + " as select * from " + Property.baseTable + " limit 0 ";
			stmt.executeUpdate(sql);
			
			sql = "create memory table if not exists " + gp.getTable()  + "_mem as select * from " + Property.baseTable + " limit 0 ";
			stmt.executeUpdate(sql);
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	
}
