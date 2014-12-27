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
public class LongHashTable { 

	private final int mask;
	
	private final long[] keys;
	private final int[] values;
	
	private int space;
	
	public LongHashTable(int bits) {
		int size = 1<<bits;
		mask = size-1;
		space = mask; //this is 1 less by design
		
		keys = new long[size];
		values = new int[size];
		int j = size;
		
	}
		
	public static boolean setItem(LongHashTable ht, long key, int value)
	{
		if (0==key || 0==ht.space) { 
			return false;
		}
				
		long block = value;
		block = (block<<32) | (0xFFFFFFFF&key);
		
		int mask = ht.mask;
		int hash = MurmurHash.hash64finalizer(key);
		
		long keyAtIdx = ht.keys[hash&mask];
		while (keyAtIdx != key && keyAtIdx != 0) { 			
			keyAtIdx = ht.keys[++hash&mask];
		}
		
		if (0 != keyAtIdx) {
			return false; //do not set item if it holds a previous value.
		}		
		
		ht.keys[hash&mask] = key;
		ht.values[hash&mask] = value;
		
		ht.space--;//gives up 1 spot as a stopper for get.
		
		return true;
	}
	
	public static int getItem(LongHashTable ht, long key) {

		int mask = ht.mask;
		int hash = MurmurHash.hash64finalizer(key);
		
		long keyAtIdx = ht.keys[hash&mask];
		while (keyAtIdx != key && keyAtIdx != 0) { 			
			keyAtIdx = ht.keys[++hash&mask];
		}
		
		return ht.values[hash&mask];
	}
	    
	public static boolean hasItem(LongHashTable ht, long key) {

		int mask = ht.mask;
		
		int hash = MurmurHash.hash64finalizer(key);
		
		long keyAtIdx = ht.keys[hash&mask];
		while (keyAtIdx != key && keyAtIdx != 0) { 			
			keyAtIdx = ht.keys[++hash&mask];
		}
				
		return 0 == keyAtIdx;
	}
	
	public static boolean replaceItem(LongHashTable ht, long key, int newValue) {

		int mask = ht.mask;
		int hash = MurmurHash.hash64finalizer(key);
		
		long keyAtIdx = ht.keys[hash&mask];
		while (keyAtIdx != key && keyAtIdx != 0) { 			
			keyAtIdx = ht.keys[++hash&mask];
		}
				
		if (0 == keyAtIdx) {
			return false; //do not set item if it holds a previous value.
		}
		
		ht.values[hash&mask] = newValue;
		return true;
	}
	
   public static void visit(LongHashTable ht, LongHashTableVisitor visitor) {
	   int j = ht.mask+1;
	   while (--j>=0) {
		   long key = ht.keys[j];
		   if (0!=key) {			   
			   visitor.visit(key,ht.values[j]);
		   }		   
	   }	   
   }	
	
}
