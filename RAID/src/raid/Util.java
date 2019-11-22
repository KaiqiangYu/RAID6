package raid;
/**
 * Operations of multiplication and inversion in finite field
 * @author Yui
 * @version 11/2019
 */
public class Util {
	
	/** used to store element in finite field*/
	private static int[] table;
	/** used to store inverse of each element in finite field*/
	public static int[] inverse_table;
	private static int[] arc_table;
	
	/**
	 * Build the table used to query the inverse of given element
	 */
	public static void Build_table() {
		table=new int[256];
		table[0] = 1;
		
		for(int i = 1; i < 255; ++i){
			table[i] = (table[i-1] << 1 ) ^ table[i-1];
			if( (table[i] & 0x100) == 0x100) { 
				table[i] ^= 0x11B;
		    }
		}

		arc_table=new int[256];  
		  
		for(int i = 0; i < 255; ++i)  
		    arc_table[ table[i] ] = i;  

		inverse_table=new int[256];  
		  
		for(int i = 1; i < 256; ++i) {  
		    int k = arc_table[i];  
		    k = 255 - k;  
		    k %= 255;  
		    inverse_table[i] = table[k];  
		}
		
		
	}
	
	/**
	 * Compute the inverse of given element
	 * @param a the given element in finite field
	 * @return the inverse of given element
	 */
	public static byte Inverse(byte a) {
		int b=a&0x8F;
		return (byte)inverse_table[b];
	}
	
	/**
	 * Compute the multiplication of two given elements a and b in finite field
	 * @param a the given element in finite field
	 * @param b the given element in finite field
	 * @return the multiplication of two given elements a and b in finite field
	 */
	public static byte Multiply(byte a, byte b) {
		byte temp[] = new byte[8];
		byte res = 0x00;
		temp[0] = a;
		for (int i=1;i<8;++i) {
			int x = temp[i-1];
			temp[i] = (byte) ((x << 1) ^ (((x & 0x80) == 0x80) ? 0x1b : 0x00));
		}
		res = (byte) ((b & 0x01) * a);
		for (int i = 1; i <= 7; i++) {
			res ^= (((b >>> i) & 0x01) * temp[i]);
		}
		return res;
		
	}
	
	

}
