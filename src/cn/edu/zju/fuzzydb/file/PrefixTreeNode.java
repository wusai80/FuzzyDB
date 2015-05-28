package cn.edu.zju.fuzzydb.file;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import cn.edu.zju.fuzzydb.db.DBConnection;
import cn.edu.zju.fuzzydb.db.MetaStore;

/**
 * prefixtree node maintains the samples in an in-memory
 * h2 table 
 * @author wusai
 *
 */
public class PrefixTreeNode {
	
	private GroupPredicate gp;

	private ArrayList<PrefixTreeNode> Child;
	
	private boolean isLeaf = true;
	
	public PrefixTreeNode(GroupPredicate predicate){
		gp = predicate;
	}
	
	
	public boolean isLeafNode(){
		return isLeaf;
	}
	
	/**
	 * split the tree node by a new group column
	 * @param groupColumn
	 */
	public void split(String groupColumn){
		if(!gp.isContained(groupColumn)){
			//verify if this is a correct group column
			try {
				String[] columns = MetaStore.getInstance().getColumns(Property.baseTable);
				for(int i=0; i<columns.length; i++){
					if(columns[i].equals(groupColumn)){  //should also check whether this is a dimension column
						
						//sort original memory table by group columns
						String sql = "select * from " + gp.getTable() + "_mem order by " + groupColumn;
						Connection conn = DBConnection.getConnection();
						conn.setAutoCommit(false);
						Statement stmt = conn.createStatement();
						Statement updateStmt = conn.createStatement();
						
						stmt.setFetchSize(Property.sampleBatchSize);
						ResultSet rs = stmt.executeQuery(sql);
						ResultSetMetaData metadata =  rs.getMetaData();
						String currentGroupValue = "";
						String currentTable = "";
						
						while(rs.next()){
							String groupValue = rs.getString(groupColumn);
							if(groupValue.equals(currentGroupValue)){
								//still data in the old group
								String insert = "insert into " + currentTable + " values (";
								
								int cols = metadata.getColumnCount();
								for(int j=1; j<=cols; j++){
									int type = metadata.getColumnType(j);
									switch(type){
									case java.sql.Types.BLOB:
									case java.sql.Types.CHAR:
									case java.sql.Types.DATE:
									case java.sql.Types.VARCHAR:
									case java.sql.Types.TIMESTAMP: insert += "'" + rs.getString(j) + "',"; break;
									default: insert += rs.getString(j) + ","; 
									
									}
									insert = insert.substring(0, insert.length()-1) + ")";
									updateStmt.addBatch(insert);
									
								}
							}
							else{
								//insert data into previous table in batch
								updateStmt.executeBatch();
								
								//create new child nodes
								currentGroupValue = groupValue;

								GroupPredicate childgp = gp.extendColumn(groupColumn, currentGroupValue);
								childgp.setStartOffset(gp.getStartOffset());
								childgp.setEndOffset(gp.getEndOffset());
								PrefixTreeNode child = new PrefixTreeNode(childgp);
															
								if(Child == null)
									Child = new ArrayList<PrefixTreeNode>();
								
								this.Child.add(child);
								currentTable = childgp.getTable()+"_mem";
								
								//create the corresponding in-memory and disk tables
								DBConnection.createNewSampleTable(childgp);
								
								//insert data into the memory table. only when the memory is full,
								//will we use the disk table.
								String insert = "insert into " + currentTable + " values (";
								
								int cols = metadata.getColumnCount();
								for(int j=1; j<=cols; j++){
									int type = metadata.getColumnType(j);
									switch(type){
									case java.sql.Types.BLOB:
									case java.sql.Types.CHAR:
									case java.sql.Types.DATE:
									case java.sql.Types.VARCHAR:
									case java.sql.Types.TIMESTAMP: insert += "'" + rs.getString(j) + "',"; break;
									default: insert += rs.getString(j) + ","; 
									
									}
									insert = insert.substring(0, insert.length()-1) + ")";
									updateStmt.addBatch(insert);
									
								}
							}
						}
						
						updateStmt.executeBatch();
						updateStmt.close();
						rs.close();
						
						//drop current memory table
						sql = "drop table if exists " + gp.getTable() + "_mem";
						stmt.executeUpdate(sql);
						//does not drop the disk table
						
						stmt.close();
						
					}
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			isLeaf = false;
		}
	}
	
	/**
	 * flush the in-memory table to disk table
	 */
	public void flush(){
		
		try {
			String sql = "insert into " + gp.getTable() + " select * from " + gp.getTable()+"_mem";
			Statement stmt = DBConnection.getConnection().createStatement();
			stmt.executeUpdate(sql);
			//delete current in-memory table
			sql = "delete from " + gp.getTable()+"_mem";
			stmt.executeUpdate(sql);
			
			sql = "update table " + Property.progressTable + " set end=" + gp.getEndOffset() + " where tablename='" +
					gp.getTable() + "'";
			stmt.executeUpdate(sql);
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public GroupPredicate getPredicate(){
		return gp;
	}
	
	public ArrayList<PrefixTreeNode> getChildren(){
		return Child;
	}
	
}
