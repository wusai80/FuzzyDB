package cn.edu.zju.fuzzydb.file;

/**
 * the default settings
 * @author wusai
 *
 */
public class Property {
	
	public static String dbName = "h2db";
	
	/**
	 * maintain how samples are maintained in different db tables.
	 */
	public static String configTable = "config";
	
	/**
	 * the table records the sampling progress
	 */
	public static String progressTable = "progress";
	
	/**
	 * original data (after joining the dimensional tables with the fact table)
	 */
	public static String baseTable = "base";
	
	public static int sampleBatchSize = 100;
	

}
