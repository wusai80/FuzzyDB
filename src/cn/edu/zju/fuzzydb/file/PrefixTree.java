package cn.edu.zju.fuzzydb.file;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import cn.edu.zju.fuzzydb.db.DBConnection;

/**
 * prefix tree is used to maintain samples in memory
 * and find the corresponding sample files from disk
 * @author wusai
 *
 */
public class PrefixTree {
	
	/**
	 * prefix root represents the original database
	 */
	private PrefixTreeNode root;
	
	/**
	 * current node that user is focusing on
	 */
	private PrefixTreeNode currentNode;
	
	/**
	 * database cursor that points to the base table
	 */
	private ResultSet baseResult;
	
	public PrefixTree(){
		GroupPredicate gp = new GroupPredicate();
		root = new PrefixTreeNode(gp);
		currentNode = root;
		try {
			Statement stmt = DBConnection.getConnection().createStatement();
			stmt.setFetchSize(Property.sampleBatchSize);
			baseResult = stmt.executeQuery("select * from " + Property.baseTable);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param Groupcolumn
	 * @param confidence
	 * @param errorRate
	 */
	public void expand(String Groupcolumn, double confidence, double errorRate){
		if(currentNode == root){
			//sample the original table
			
		}
		else{
			//first try to use memory table to answer
			
			
			//if cannot meet the requirement, start sampling and splitting the tree node
			
		}
	}
	
	/**
	 * sampling the database
	 */
	public void performSampling(String group){

		currentNode.split(group);
		ArrayList<String> sqlCmd = new ArrayList<String>();
		for(int i=0; i<Property.sampleBatchSize; i++)
			insert(sqlCmd);
	}
	
	/**
	 * insert the tuple referred by current baseResult into the prefix tree
	 * this may be slow due to the comparisons
	 */
	private void insert(ArrayList<String> sqlCmd){
		try {
			PrefixTreeNode searchNode = root;
			while(baseResult.next()){
				if(searchNode.isLeafNode()){
					//insert into the corresponding memory table
					String insert = "insert into " + searchNode.getPredicate().getTable() + "_mem values (";
					ResultSetMetaData metadata = baseResult.getMetaData();
					int cols = metadata.getColumnCount();
					for(int j=1; j<=cols; j++){
						int type = metadata.getColumnType(j);
						switch(type){
						case java.sql.Types.BLOB:
						case java.sql.Types.CHAR:
						case java.sql.Types.DATE:
						case java.sql.Types.VARCHAR:
						case java.sql.Types.TIMESTAMP: insert += "'" + baseResult.getString(j) + "',"; break;
						default: insert += baseResult.getString(j) + ","; 
						
						}
						insert = insert.substring(0, insert.length()-1) + ")";
						sqlCmd.add(insert);
						
					}
				}
				else{
					ArrayList<PrefixTreeNode> children = searchNode.getChildren();
					for(PrefixTreeNode node : children){
						GroupPredicate gp = node.getPredicate();
						int size = gp.getDimensionNumber();
						String columnName = gp.getDimensionAt(size-1);
						String value = baseResult.getString(columnName); //all values are considered as string values temporarily
						if(value.equals(gp.getValueAt(size-1))){
							searchNode = node;
							break;
						}
					}
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
