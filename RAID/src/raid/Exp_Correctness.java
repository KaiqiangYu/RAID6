package raid;
/**
 * Experiment codes used to test the correctness of various operations
 * @author Yui
 * @version 11/2019
 */
public class Exp_Correctness {
	
	public static void main(String[] args) {
		Storage test = new Storage(1024*16,6);
		test.Initial();
		System.out.println("Initialize System !!!");
		
		System.out.println("Rebuild a data chunk !!!");
		
		int stripe_id=5; int chuck_id=4;
		
		byte a[]=test.Read_Chuck(stripe_id,chuck_id);
		test.Rebuild_Chuck(stripe_id,chuck_id, 0);
		byte b[]=test.Read_Chuck(stripe_id,chuck_id);
		boolean check=true;
		for (int i=0;i<b.length;++i) {
			if(a[i]!=b[i]) {
				check=false;
				System.out.println("ERROR !!!");
				break;
			}
		}
		
		test.Rebuild_Chuck(stripe_id,chuck_id, 1);
		byte c[]=test.Read_Chuck(stripe_id,chuck_id);
		for (int i=0;i<b.length;++i) {
			if(c[i]!=b[i]) {
				check=false;
				System.out.println("ERROR !!!");
				break;
			}
		}
		
		System.out.println("rebuild two chunks !!!");
		
		int stripe_id1=2; int stripe_id2=3; int chunk_id1=0;
		byte d[]=test.Read_Chuck(stripe_id1,chunk_id1);
		byte e[]=test.Read_Chuck(stripe_id2,chunk_id1);
		test.Rebuild_Chucks(stripe_id1, stripe_id2, chunk_id1);
		byte f[]=test.Read_Chuck(stripe_id1,chunk_id1);
		byte g[]=test.Read_Chuck(stripe_id2,chunk_id1);
		
		for (int i=0;i<b.length;++i) {
			if(d[i]!=f[i]||e[i]!=g[i]) {
				check=false;
				System.out.println("ERROR !!!");
				break;
			}
		}
		
		if(check)
			System.out.println("Correct !!!");
		else
			System.out.println("ERROR !!!");
		
	}

}
