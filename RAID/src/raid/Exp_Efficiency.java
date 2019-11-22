package raid;
/**
 * Experiment codes used to test the efficiency of various operations
 * @author Yui
 * @version 11/2019
 */
public class Exp_Efficiency {
	
	public static void main(String[] args) {
		Storage test = new Storage(1024*64,6);
		test.Initial();
		System.out.println("Initialize System !!!");
		
		System.out.println("Rebuild a data chunk !!!");
		
		int stripe_id=5; int chuck_id=4;
		int stripe_id1=2; int stripe_id2=3; int chunk_id1=0;
		double res1=0;
		double res2=0;
		double res3=0;
		double res4=0;
		double res5=0;
		
		for(int i=0;i<300;++i) {
			double a=System.currentTimeMillis();
			test.Rebuild_Chuck(stripe_id,chuck_id, 0);
			double b=System.currentTimeMillis();
			res1+=(b-a);
		}
		
		for(int i=0;i<300;++i) {
			double a=System.currentTimeMillis();
			test.Rebuild_Chuck(stripe_id,chuck_id, 1);
			double b=System.currentTimeMillis();
			res2+=(b-a);
		}

		
		for(int i=0;i<300;++i) {
			double a=System.currentTimeMillis();
			test.Rebuild_Chucks(stripe_id1, stripe_id2, chunk_id1);
			double b=System.currentTimeMillis();
			res3+=(b-a);
		}
		
		for(int i=0;i<10;++i) {
			double a=System.currentTimeMillis();
			test.Rebuild_Stripe(stripe_id1);
			double b=System.currentTimeMillis();
			res4+=(b-a);
		}
		
		for(int i=0;i<10;++i) {
			double a=System.currentTimeMillis();
			test.Rebuild_Stripe(stripe_id1,stripe_id2);
			double b=System.currentTimeMillis();
			res5+=(b-a);
		}
		
		System.out.println("rebuild by first parity "+ res1/300);
		System.out.println("rebuild by second parity "+ res2/300);
		System.out.println("rebuild by two parity "+ res3/300);
		System.out.println("rebuild by a stripe "+ res4/10);
		System.out.println("rebuild by two stripe "+ res5/10);
		

		
	}

}
