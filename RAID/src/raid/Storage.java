package raid;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import raid.Config;

/**
 * Implementation of RAID-6 based distributed system
 * @author Yui
 * @version 11/2019
 */
public class Storage {

	/** chunk size */
	private final int CHUCK_SIZE; 
	/** The number of stripes */
	private final int STRIPES_NUM;
	/** The length of address for chunk index */
	private final int ADD_CHUCK_LEN;
	/** The length of address for storage node index */
	private final int ADD_STRIPES_LEN;
	/** The number of chunk */
	private final int ADD_CHUCK_NUM;
	/** The number of storage nodes */
	private final int ADD_STRIPES_NUM;
	/** The number of chunk group */
	private final int GROUP_NUM;
	/** The length of address for group index */
	private final int GROUP_LEN;
	/** The array used to generate the second parity Q*/
	private final byte[] GEN;
	/** The inverse of each element in array GEN*/
	private final byte[] Inverse_GEN;

	/**
	 * construction function used to initialize the system
	 * @param chuck_size chuck size
	 * @param stripes the number of stripes
	 */
	public Storage(int chuck_size, int stripes) {
		this.CHUCK_SIZE = chuck_size;
		this.STRIPES_NUM = stripes;
		ADD_CHUCK_LEN = DexToBin(CHUCK_SIZE);
		ADD_STRIPES_LEN = DexToBin(STRIPES_NUM + 2);
		GROUP_LEN = Config.ADD_WEITH - ADD_CHUCK_LEN - ADD_STRIPES_LEN;
		GROUP_NUM = BinToDex(GROUP_LEN);
		ADD_CHUCK_NUM=BinToDex(ADD_CHUCK_LEN);
		ADD_STRIPES_NUM=BinToDex(ADD_STRIPES_LEN);
		Util.Build_table();
		GEN=new byte[255];
		GEN[0]=(byte)0x01;
		Inverse_GEN=new byte[255];
		Inverse_GEN[0]=Util.Inverse(GEN[0]);
		for(int i=1;i<255;++i) {
			GEN[i]=Util.Multiply(GEN[i-1], (byte)0x02);
			Inverse_GEN[i]=Util.Inverse(GEN[i]);
		}
		

		if (GROUP_LEN <= 0) {
			System.out.println("ERROR !");
			return;
		}
		
		//Initial();
	

	}
	
	
	/**
	 * Initialize the storage by constructing storage nodes (folders) and chunks (files)
	 *  
	 */
	public void Initial() {

		for (int i = 0; i < STRIPES_NUM + 2; ++i) {
			File stripe = new File("./RAID/stripe" + i);
			if (!stripe.exists()) {
				stripe.mkdir();
			}
		}
		try {
			for (int i = 0; i < GROUP_NUM; ++i) {
				
				int temp_parity = (i * 2) % (STRIPES_NUM + 2);
				byte[][] data = new byte[STRIPES_NUM][CHUCK_SIZE];
				int temp_count=0;
				
				for (int j = 0; j < STRIPES_NUM + 2; ++j) {
					if (j == temp_parity || j == temp_parity + 1)
						continue;
					DataOutputStream out = new DataOutputStream(
							new FileOutputStream("./RAID/stripe" + j + "/chuck" + i + ".yui"));
					byte b[] = new byte[CHUCK_SIZE];
					for (int k = 0; k < CHUCK_SIZE; ++k) {
						b[k] = (byte) (256 * Math.random());
					}
					data[temp_count]=b;
					temp_count++;
					
					out.write(b);
					out.flush();
					out.close();
				}
				
				for (int j=temp_parity;j<temp_parity+2;++j) {
					DataOutputStream out = new DataOutputStream(
							new FileOutputStream("./RAID/stripe" + j + "/chuck" + i + ".yui"));
					byte b[] = new byte[CHUCK_SIZE];
					
					if(j==temp_parity) {
						for(int k=0;k<CHUCK_SIZE;++k) {
							byte temp=(byte)0x00;
							for(int m=0;m<STRIPES_NUM;++m) {
								temp=(byte) (temp^data[m][k]);
							}
							b[k]=temp;

						}
					}else {
						for(int k=0;k<CHUCK_SIZE;++k) {
							byte temp=(byte)0x00;
							for(int m=0;m<STRIPES_NUM;++m) {
								temp=(byte) (temp^(Util.Multiply(data[m][k], GEN[m])));
							}
							b[k]=temp;
							
							
						}
						
					}
					
					out.write(b);
					out.flush();
					out.close();
					
				}
				
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
		System.out.println("System Initialization Finshed !!!");

	}
	
	private int DexToBin(int num) {
		int res = 1;
		while (num > 2) {
			num = num / 2;
			res++;
		}
		return res;
	}

	private int BinToDex(int num) {
		int res = 1;
		while (num > 0) {
			res *= 2;
			num--;
		}
		return res;
	}
	
	/**
	 * RAID mapping : translate the logical address to physical address (chuck id, storage node id, group id)
	 * @param address logical address
	 * @return chuck id, storage node id, group id
	 */
	private int[] Address_Mapping(int address) {
		int chuck_id=0, stripe_id=0, group_id=0;
		chuck_id=address^(ADD_CHUCK_NUM-1);
		stripe_id=(address>>>ADD_CHUCK_LEN)^(ADD_STRIPES_NUM-1);
		group_id=(address>>>ADD_STRIPES_LEN)^(GROUP_NUM-1);
		int[] res= {chuck_id,stripe_id,group_id};
		return res;
	}
	
	/**
	 * Write data into a chuck without rebuilding parities
	 * @param stripe_id storage node id
	 * @param chuck_id chunk id
	 * @param content content need to be written
	 * @return true(success) or false(fail)
	 */
	private boolean Write_Chuck(int stripe_id,int chuck_id,byte[] content) {
		if(stripe_id>=STRIPES_NUM+2||chuck_id>=GROUP_NUM||stripe_id<0||chuck_id<0) {
			System.out.println("Invaild Address !!!");
			return false;
		}
		String file="./RAID/stripe" + stripe_id+"/chuck"+chuck_id+".yui";
		try {
			
			DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
			out.write(content);
			out.flush();
			out.close();
			return true;
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("ERROR !!!");
		return false;
	}
	
	/**
	 * Read data in a specific chunk
	 * @param stripe_id target storage node id
	 * @param chuck_id target chunk id
	 * @return the target content
	 */
	public byte[] Read_Chuck(int stripe_id,int chuck_id) {
		if(stripe_id>=STRIPES_NUM+2||chuck_id>=GROUP_NUM||stripe_id<0||chuck_id<0) {
			System.out.println("Invaild Address !!!");
			return null;
		}
		String file="./RAID/stripe" + stripe_id+"/chuck"+chuck_id+".yui";
		try {
			byte res[]=new byte[CHUCK_SIZE];
			DataInputStream in = new DataInputStream(new FileInputStream(file));
			in.read(res);
			in.close();
			return res;
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("READ ERROR !!!");
		return null;
		
	}
	
	/**
	 * Read data in the specific address
	 * @param address the target address
	 * @return the target content
	 */
	public byte Read(int address) {
		if(address>Config.MAX_ADD||address<0) {
			System.out.println("Invaild Address !!!");
			return (byte)0x00;
		}
		
		int phy_addr[]=Address_Mapping(address);
		String file="./RAID/stripe" + phy_addr[1]+"/chuck"+phy_addr[2]+".yui";
		
		File block = new File(file);
		if(!block.exists()) {
			System.out.println("ERROR ! FILE DELECTION !!!");
			return (byte)0x00;
		}
		
		try {
			byte res[]=new byte[1];
			DataInputStream in = new DataInputStream(new FileInputStream(file));
			in.skip(phy_addr[0]);
			in.read(res, 0,1);
			return res[0];
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("READ ERROR !!!");
		return (byte)0x00;		
	}
	
	/**
	 * Write data in the specific address
	 * @param address the target address
	 * @param content the content needed to be written
	 * @return true(success) or false(fail)
	 */
	public boolean Write(int address, byte content) {
		if(address>Config.MAX_ADD||address<0) {
			System.out.println("Invaild Address !!!");
			return false;
		}
		
		int phy_addr[]=Address_Mapping(address);
		int temp_parity=(phy_addr[2]*2)%(STRIPES_NUM+2);
		if(temp_parity==phy_addr[1]||temp_parity==phy_addr[1]-1) {
			System.out.println("Parity Chuck cannot be written by user !!!");
			return false;
		}
		
		String file="./RAID/stripe" + phy_addr[1]+"/chuck"+phy_addr[2]+".yui";		
		File block = new File(file);
		if(!block.exists()) {
			System.out.println("ERROR ! FILE DELECTION !!!");
			return false;
		}
		
		try {
			byte history[]=new byte[STRIPES_NUM];
			byte update[]= {(byte)0x00,(byte)0x00};
			int temp_count=0;
			for(int i=0;i<STRIPES_NUM+2;++i) {
				if(i==temp_parity||i==temp_parity+1) {
					continue;
				}else if(i==phy_addr[1]) {
					history[temp_count]=content;
					temp_count++;
					continue;
				}
				String file1="./RAID/stripe" + i+"/chuck"+phy_addr[2]+".yui";
				DataInputStream in = new DataInputStream(new FileInputStream(file1));
				in.skip(phy_addr[0]);
				in.read(history, temp_count,1);
				temp_count++;
				in.close();
			}
			
			for(int i=0;i<STRIPES_NUM;++i) {
				update[0]=(byte) (update[0]^history[i]);
				update[1]=(byte) (update[1]^(Util.Multiply(history[i], update[1])));
			}
			
			byte write_content[][]=new byte[3][];
			write_content[0]=Read_Chuck(temp_parity, phy_addr[2]);
			write_content[1]=Read_Chuck(temp_parity+1, phy_addr[2]);
			write_content[2]=Read_Chuck(phy_addr[1], phy_addr[2]);
			
			write_content[0][phy_addr[0]]=update[0];
			write_content[1][phy_addr[0]]=update[1];
			write_content[2][phy_addr[0]]=content;
			
			Write_Chuck(temp_parity, phy_addr[2], write_content[0]);
			Write_Chuck(temp_parity+1, phy_addr[2], write_content[1]);
			Write_Chuck(phy_addr[1], phy_addr[2], write_content[2]);
			
			return true;
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("READ ERROR !!!");
		return false;		
	}
	
	/**
	 * Rebuild the one arbitrary fail chunks
	 * @param stripe_id the first fail storage node id
	 * @param chuck_id the fail chunk id
	 * @param parity 0 rebuild based on the first parity P; 1 rebuild based on the second parity Q
	 * @return true(success to rebuild) or false(fail to rebuild)
	 */
	public boolean Rebuild_Chuck(int stripe_id, int chuck_id, int parity) {
		if(stripe_id>=STRIPES_NUM+2||chuck_id>=GROUP_NUM||stripe_id<0||chuck_id<0) {
			System.out.println("Invaild Address !!!");
			return false;
		}
		int temp_parity=(chuck_id*2)%(STRIPES_NUM+2);
		if(stripe_id==temp_parity) {
			//System.out.println("Rebuild the first parity chuck !!!");
			byte data[][] = new byte[STRIPES_NUM][];
			int temp_count=0;
			for (int i=0;i<STRIPES_NUM+2;++i) {
				if(i==temp_parity||i==temp_parity+1) {
					continue;
				}
				data[temp_count]=Read_Chuck(i, chuck_id);
				temp_count++;
			}
			byte update[] = new byte[CHUCK_SIZE];
			for (int i=0;i<CHUCK_SIZE;++i) {
				byte temp=(byte)0x00;
				for(int j=0;j<STRIPES_NUM;++j) {
					temp=(byte) (temp^data[j][i]);
				}
				update[i]=temp;
			}
			Write_Chuck(stripe_id,chuck_id,update);
			return true;
			
		}else if(stripe_id==temp_parity+1) {
			//System.out.println("Rebuild the second parity chuck !!!");
			byte data[][] = new byte[STRIPES_NUM][];
			int temp_count=0;
			for (int i=0;i<STRIPES_NUM+2;++i) {
				if(i==temp_parity||i==temp_parity+1) {
					continue;
				}
				data[temp_count]=Read_Chuck(i, chuck_id);
				temp_count++;
			}
			byte update[] = new byte[CHUCK_SIZE];
			for (int i=0;i<CHUCK_SIZE;++i) {
				byte temp=(byte)0x00;
				for(int j=0;j<STRIPES_NUM;++j) {
					temp=(byte) (temp^(Util.Multiply(data[j][i], GEN[j])));
				}
				update[i]=temp;
			}
			Write_Chuck(stripe_id,chuck_id,update);
			return true;
			
		}else if(parity==0){
			//System.out.println("Rebuild the data chuck by using the first parity!!!");
			byte data[][] = new byte[STRIPES_NUM][];
			int temp_count=0;
			for (int i=0;i<STRIPES_NUM+2;++i) {
				if(i==stripe_id||i==temp_parity+1) {
					continue;
				}
				data[temp_count]=Read_Chuck(i, chuck_id);
				temp_count++;
			}
			byte update[] = new byte[CHUCK_SIZE];
			for (int i=0;i<CHUCK_SIZE;++i) {
				byte temp=(byte)0x00;
				for(int j=0;j<STRIPES_NUM;++j) {
					temp=(byte) (temp^data[j][i]);
				}
				update[i]=temp;
			}
			Write_Chuck(stripe_id,chuck_id,update);
			return true;
			
		}else {
			//System.out.println("Rebuild the data chuck by using the second parity!!!");
			byte data[][] = new byte[STRIPES_NUM][];
			int temp_count=0;
			int index=0;
			for (int i=0;i<STRIPES_NUM+2;++i) {
				if(i==temp_parity||i==temp_parity+1||i==stripe_id) {
					if(i==stripe_id) {
						index=temp_count;
						temp_count++;
					}
					continue;
				}
				data[temp_count]=Read_Chuck(i, chuck_id);
				temp_count++;
			}
			byte[] par=Read_Chuck(temp_parity+1, chuck_id);
			byte update[] = new byte[CHUCK_SIZE];
			
			for (int i=0;i<CHUCK_SIZE;++i) {
				byte temp=(byte)0x00;
				for(int j=0;j<STRIPES_NUM;++j) {
					if(j==index)
						continue;
					temp=(byte) (temp^(Util.Multiply(data[j][i], GEN[j])));
				}
				temp=(byte)(temp^par[i]);
				temp=Util.Multiply(temp, Inverse_GEN[index]);
				update[i]=temp;
			}
			Write_Chuck(stripe_id,chuck_id,update);
			return true;
		}
		
	}
	
	
	/**
	 * Rebuild the two arbitrary fail chunks
	 * @param stripe_id1 the first fail storage node id
	 * @param stripe_id2 the second fail storage node id
	 * @param chuck_id the fail chunk id
	 * @return true(success to rebuild) or false(fail to rebuild)
	 */
	public boolean Rebuild_Chucks(int stripe_id1, int stripe_id2, int chuck_id) {
		if(stripe_id1>=STRIPES_NUM+2||stripe_id2>=STRIPES_NUM+2||chuck_id>=GROUP_NUM
				||stripe_id1<0||stripe_id2<0||chuck_id<0||stripe_id1==stripe_id2) {
			System.out.println("Invaild Address !!!");
			return false;
		}
		int temp_min=(stripe_id1<stripe_id2)?stripe_id1:stripe_id2;
		int temp_max=(stripe_id1>stripe_id2)?stripe_id1:stripe_id2;
		stripe_id1=temp_min;
		stripe_id2=temp_max;
		int temp_parity=(chuck_id*2)%(STRIPES_NUM+2);
		if(stripe_id1==temp_parity&&stripe_id2==temp_parity+1) {
			return Rebuild_Chuck(stripe_id1, chuck_id, 0)
					&&Rebuild_Chuck(stripe_id2, chuck_id, 0); 
		}else if(stripe_id1==temp_parity||stripe_id2==temp_parity) {
			boolean a1=false;
			boolean b1=false;
			if(stripe_id1==temp_parity) {
				a1=Rebuild_Chuck(stripe_id2, chuck_id, 1);
				b1=Rebuild_Chuck(stripe_id1, chuck_id, 0);
			}else {
				a1=Rebuild_Chuck(stripe_id1, chuck_id, 1);
				b1=Rebuild_Chuck(stripe_id2, chuck_id, 0);
			}
			return a1&&b1;
				
		}else if(stripe_id1==temp_parity+1||stripe_id2==temp_parity+1) {
			boolean a1=false;
			boolean b1=false;
			if(stripe_id1==temp_parity+1) {
				a1=Rebuild_Chuck(stripe_id2, chuck_id, 0);
				b1=Rebuild_Chuck(stripe_id1, chuck_id, 0);
			}else {
				a1=Rebuild_Chuck(stripe_id1, chuck_id, 0);
				b1=Rebuild_Chuck(stripe_id2, chuck_id, 0);
			}
			return a1&&b1;
		}else {
			//System.out.println("Rebuild two data chunks !!!");
			byte data[][] = new byte[STRIPES_NUM][];
			int temp_count=0;
			int index1=0;
			int index2=0;
			for (int i=0;i<STRIPES_NUM+2;++i) {
				if(i==temp_parity||i==temp_parity+1||i==stripe_id1||i==stripe_id2) {
					if(i==stripe_id1) {
						index1=temp_count;
						temp_count++;
					}else if(i==stripe_id2) {
						index2=temp_count;
						temp_count++;
					}
					continue;
				}
				data[temp_count]=Read_Chuck(i, chuck_id);
				temp_count++;
			}
			byte[] par1=Read_Chuck(temp_parity, chuck_id);
			byte[] par2=Read_Chuck(temp_parity+1, chuck_id);
			byte update1[] = new byte[CHUCK_SIZE];
			byte update2[] = new byte[CHUCK_SIZE];
			for (int i=0;i<CHUCK_SIZE;++i) {
				byte temp1=(byte)0x00;
				byte temp2=(byte)0x00;
				for(int j=0;j<STRIPES_NUM;++j) {
					if(j!=index1&&j!=index2) {
						temp1=(byte) (temp1^data[j][i]);
						temp2=(byte) (temp2^(Util.Multiply(data[j][i], GEN[j])));						
					}											
				}
				temp1=(byte)(temp1^par1[i]);
				temp2=(byte)(temp2^par2[i]);
				byte temp22=Util.Multiply(Util.Inverse((byte) (GEN[index1]^GEN[index2])), (byte) (temp2^(Util.Multiply(GEN[index1], temp1))));
				update2[i]=(byte) (temp22);
				update1[i]=(byte) (temp1^update2[i]);
			}
			Write_Chuck(stripe_id1,chuck_id,update1);
			Write_Chuck(stripe_id2,chuck_id,update2);
			return true;
		}
	}
	
	/**
	 * Rebuild the one arbitrary fail storage node
	 * @param stripe_id the fail storage node id
	 * @return true(success to rebuild) or false(fail to rebuild)
	 */
	public boolean Rebuild_Stripe(int stripe_id) {
		boolean res=false;
		for (int i=0;i<GROUP_NUM;++i) {
			res=Rebuild_Chuck(stripe_id, i, 0);
			if(res==false)
				return false;
		}
		return true;		
	}
	
	/**
	 * Rebuild the two arbitrary fail storage nodes
	 * @param stripe_id1 the first fail storage node id
	 * @param stripe_id2 the second fail storage node id
	 * @return true(success to rebuild) or false(fail to rebuild)
	 */
	public boolean Rebuild_Stripe(int stripe_id1, int stripe_id2) {
		boolean res=false;
		for (int i=0;i<GROUP_NUM;++i) {
			res=Rebuild_Chucks(stripe_id1, stripe_id2, i);
			if(res==false)
				return false;
		}
		return true;	
	}
	
	/**
	 * Check the validity of data stored in a specific chunk
	 * @param stripe_id storage node id
	 * @param chuck_id chunk id
	 * @return true(valid) or false(invalid)
	 */
	public boolean Check_Chuck(int stripe_id, int chuck_id) {
		if(stripe_id>=STRIPES_NUM+2||chuck_id>=GROUP_NUM||stripe_id<0||chuck_id<0) {
			System.out.println("Invaild Address !!!");
			return false;
		}
		int temp_parity=(chuck_id*2)%(STRIPES_NUM+2);
		if(stripe_id==temp_parity) {
			//System.out.println("Check the first parity chuck !!!");
			byte data[][] = new byte[STRIPES_NUM][];
			int temp_count=0;
			for (int i=0;i<STRIPES_NUM+2;++i) {
				if(i==temp_parity||i==temp_parity+1) {
					continue;
				}
				data[temp_count]=Read_Chuck(i, chuck_id);
				temp_count++;
			}
			byte history[]=Read_Chuck(stripe_id, chuck_id);
			for (int i=0;i<CHUCK_SIZE;++i) {
				byte temp=(byte)0x00;
				for(int j=0;j<STRIPES_NUM;++j) {
					temp=(byte) (temp^data[j][i]);
				}
				if(history[i]!=temp) {
					return false;
				}
			}
			return true;
			
		}else if(stripe_id==temp_parity+1) {
			//System.out.println("Check the second parity chuck !!!");
			byte data[][] = new byte[STRIPES_NUM][];
			int temp_count=0;
			for (int i=0;i<STRIPES_NUM+2;++i) {
				if(i==temp_parity||i==temp_parity+1) {
					continue;
				}
				data[temp_count]=Read_Chuck(i, chuck_id);
				temp_count++;
			}
			byte update[] = Read_Chuck(stripe_id, chuck_id);
			for (int i=0;i<CHUCK_SIZE;++i) {
				byte temp=(byte)0x00;
				for(int j=0;j<STRIPES_NUM;++j) {
					temp=(byte) (temp^(Util.Multiply(data[j][i], GEN[j])));
				}
				if(update[i]!=temp)
					return false;
			}
			return true;
			
		}else {
			//System.out.println("Check the data chuck by using the first parity!!!");
			byte data[][] = new byte[STRIPES_NUM][];
			int temp_count=0;
			for (int i=0;i<STRIPES_NUM+2;++i) {
				if(i==stripe_id||i==temp_parity+1) {
					continue;
				}
				data[temp_count]=Read_Chuck(i, chuck_id);
				temp_count++;
			}
			byte update[] = Read_Chuck(stripe_id, chuck_id);
			for (int i=0;i<CHUCK_SIZE;++i) {
				byte temp=(byte)0x00;
				for(int j=0;j<STRIPES_NUM;++j) {
					temp=(byte) (temp^data[j][i]);
				}
				if(update[i]!=temp)
					return false;
			}
			return true;
		}
	}
	
	/**
	 * Check validity of data stored in the specific storage node
	 * @param stripe_id storage node id
	 * @return true(valid) or false(invalid)
	 */
	public boolean Check_Stripe(int stripe_id) {
		boolean res=false;
		for(int i=0;i<GROUP_NUM;++i) {
			res=Check_Chuck(stripe_id, i);
			if(res==false)
				return res;
		}
		return res;
	}


}
