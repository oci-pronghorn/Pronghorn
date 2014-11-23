package com.ociweb.pronghorn.ring.util;


/**
 * Non-Thread safe simple fast hash for int to int mapping.
 * 
 * No set is allowed unless no previous value is found.
 * To remove previous value clear must be called.
 * 
 * @author Nathan Tippy
 *
 *  TODO: needs unit tests urgently.
 *
 */
public class IntHashTable {

	private final int mask;
	private final long[] data;
	private int memberCount = 0;
	
	public IntHashTable(int bits) {
		int size = 1<<bits;
		mask = size-1;
		
		data = new long[size];
		int j = size;		
	}
		
	public static boolean setItem(IntHashTable ht, int key, int value)
	{
		if (ht.memberCount>=ht.mask) { //gives up 1 spot as a stopper for get.
			return false;
		}
		
		long block =  (((long)value)<<32) | ((long)key);
		
		int mask = ht.mask;
		int hash = MurmurHash.hash32finalizer(key);
		int temp = (int)ht.data[hash&mask];//just the lower int.
		while (temp != key && temp != 0) { 			
			temp = (int)ht.data[++hash&mask];
		}
		
		if (0 != temp) {
			return false; //do not set item if it holds a previous value.
		}
		
		ht.data[hash&mask] = block;
		ht.memberCount++;
		return true;
	}
	
	public static int getItem(IntHashTable ht, int key) {

		int mask = ht.mask;
		int hash = MurmurHash.hash32finalizer(key);
		int temp = (int)ht.data[hash&mask];//just the lower int.
		while (temp != key && temp != 0) { 			
			temp = (int)ht.data[++hash&mask];
		}
		
		return (int)(temp >> 32);
	}
	    
	public static void clearItem(IntHashTable ht, int key) {

		int mask = ht.mask;
		int hash = MurmurHash.hash32finalizer(key);
		int temp = (int)ht.data[hash&mask];//just the lower int.
		while (temp != key && temp != 0) { 			
			temp = (int)ht.data[++hash&mask];
		}
		ht.data[hash&mask] = 0;
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
