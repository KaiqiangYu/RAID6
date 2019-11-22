package raid;
/**
 * Configuration of RAID-6 based storage system
 * @author Yui
 * @version 11/2019
 */
public class Config {
	
	/** The unite size of chunk with default 1KB */
	public static final int CHUNK_SIZE = 1024;
	/** The length of address with default 25 (2^25=32MB) */
	public static final int ADD_WEITH = 25;
	/** The maximum address with default 2^25-1 */
	public static final int MAX_ADD=0x1FFFFFF;
	

}
