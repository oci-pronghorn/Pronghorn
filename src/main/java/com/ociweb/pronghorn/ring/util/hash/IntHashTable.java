package com.ociweb.pronghorn.ring.util.hash;


/**
 * Non-Thread safe simple fast hash for int to int mapping.
 * 
 * No set is allowed unless no previous value is found.
 * To change previous value replace must be called.
 * Remove can not be supported.
 * Key must not be zero.
 * 
 * @author Nathan Tippy
 *
 */
public class IntHashTable {

	private final int mask;
	private final long[] data;
	private int space;
	
	public IntHashTable(int bits) {
		int size = 1<<bits;
		mask = size-1;
		space = mask; //this is 1 less by design
		
		data = new long[size];
		int j = size;
		
	}
		
	public static boolean setItem(IntHashTable ht, int key, int value)
	{
		if (0==key || 0==ht.space) { 
			return false;
		}
				
		long block = value;
		block = (block<<32) | (0xFFFFFFFF&key);
		
		int mask = ht.mask;
		int hash = MurmurHash.hash32finalizer(key);
		int temp = (int)ht.data[hash&mask];//just the lower int.
		while (temp != key && temp != 0) { 			
			temp = (int)ht.data[++hash & mask];
		}
		
		if (0 != temp) {
			return false; //do not set item if it holds a previous value.
		}		
		
		ht.data[hash&mask] = block;
		ht.space--;//gives up 1 spot as a stopper for get.
		
		return true;
	}
	
	public static int getItem(IntHashTable ht, int key) {

		int mask = ht.mask;
		int hash = MurmurHash.hash32finalizer(key);
		long block = ht.data[hash & mask];
		while (((int)block) != key && block != 0) { 			
			block = ht.data[++hash & mask];
		}
		return (int)(block >> 32);
	}
	    
	public static boolean hasItem(IntHashTable ht, int key) {

		int mask = ht.mask;
		int hash = MurmurHash.hash32finalizer(key);
		long block = ht.data[hash & mask];
		while (((int)block) != key && block != 0) { 			
			block = ht.data[++hash & mask];
		}
		return 0==block;
	}
	
	public static boolean replaceItem(IntHashTable ht, int key, int newValue) {

		int mask = ht.mask;
		int hash = MurmurHash.hash32finalizer(key);
		int temp = (int)ht.data[hash&mask];//just the lower int.
		while (temp != key && temp != 0) { 			
			temp = (int)ht.data[++hash & mask];
		}
		if (0 == temp) {
			return false; //do not set item if it holds a previous value.
		}
		
		long block = newValue;
		block = (block<<32) | (0xFFFFFFFF&key);
		ht.data[hash&mask] = block;
		return true;
	}
	
   public static void visit(IntHashTable ht, IntHashTableVisitor visitor) {
	   int j = ht.mask+1;
	   while (--j>=0) {
		   long block = ht.data[j];
		   if (0!=block) {
			   int key = (int)block;
			   int value = (int)(block>>32);			   
			   visitor.visit(key,value);
		   }		   
	   }	   
   }	
	
}
