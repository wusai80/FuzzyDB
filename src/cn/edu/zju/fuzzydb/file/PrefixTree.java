package cn.edu.zju.fuzzydb.file;

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
	
	public PrefixTree(){
		GroupPredicate gp = new GroupPredicate();
		root = new PrefixTreeNode(gp);
	}
	
	/**
	 * sampling the database
	 */
	public void performSampling(){
		
	}
	
	
}
