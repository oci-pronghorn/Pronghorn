package com.ociweb.pronghorn.pipe.util.hash;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private static final Logger logger = LoggerFactory.getLogger(IntHashTable.class);
	
	public static IntHashTable EMPTY = new IntHashTable(0);
	
	private final int mask;
	private final long[] data;
	private int space;
	private final int bits;
	
	public static IntHashTable newTableExpectingCount(int count) {		
		return new IntHashTable((int) (1+Math.ceil(Math.log(count)/Math.log(2)) ));
	}
	
	public IntHashTable(int bits) {
		this.bits = bits;
		int size = 1<<bits;
		mask = size-1;
		
		space = mask; //this is 1 less by design		
		data = new long[size];
	}
	
	public static int computeBits(int count) {
		return (int)Math.ceil(Math.log(count)/Math.log(2));
	}	
	
	public static void clear(IntHashTable ht) {
		ht.space = ht.mask;
		Arrays.fill(ht.data, 0);
	}
	
	public static int size(IntHashTable ht) {
		return ht.mask+1;
	}
	
	public static int count(IntHashTable ht) {
		return ht.mask - ht.space;
	}
	
	
	public static boolean isEmpty(IntHashTable ht) {
		return ht.space == ht.mask;
	}
		
	public long memoryConsumed() {
	    return 4 + 4 + (data.length*8);
	}
	
	public static boolean setItem(IntHashTable ht, int key, int value)
	{
		if (0==key || 0==ht.space) { 
			return false;
		}
				
		long block = value;
		block = (block<<32) | (0xFFFFFFFFL&key);

		int mask = ht.mask;
		int hash = MurmurHash.hash32finalizer(key);
		int temp = (int)ht.data[hash&mask];//just the lower int.
		while (temp != key && temp != 0) {
			temp = (int)ht.data[++hash & mask];
		}
		
		if (0 != temp) {
		//	System.err.println("held prev value of "+temp+" when setting "+value);
			return false; //do not set item if it holds a previous value.
		}		
		
		ht.data[hash&mask] = block;
		ht.space--;//gives up 1 spot as a stopper for get.
		
		return true;
	}
	
	/**
	 * returns zero if the value is not found otherwise it returns the value.
	 * If zero was set as the value there is no way to tell the difference without calling hasItem
	 * @param ht
	 * @param key
	 */
	public static int getItem(IntHashTable ht, int key) {
		int hash = MurmurHash.hash32finalizer(key);
		return (int)(scanForItem(key, ht.mask, hash, ht.data, ht.data[hash & ht.mask]) >> 32);
	}

	private static long scanForItem(int key, int mask, int hash, long[] data2, long block) {
		
		while ((block != 0) && ((int)block) != key) { 			
			block = data2[++hash & mask];
		}
		return block;
	}
	    
	public static boolean hasItem(IntHashTable ht, int key) {

		int mask = ht.mask;
		int hash = MurmurHash.hash32finalizer(key);
		long block = ht.data[hash & mask];
		while (((int)block) != key && block != 0) { 			
			block = ht.data[++hash & mask];
		}
		return 0!=block; 
	}
	
	public static boolean replaceItem(IntHashTable ht, int key, int newValue) {

		int mask = ht.mask;
		int hash = MurmurHash.hash32finalizer(key);
		int temp = (int)ht.data[hash&mask];//just the lower int.
		while (temp != key && temp != 0) { 			
			temp = (int)ht.data[++hash & mask];
		}
		if (0 == temp) {
			return false; //do not set item if it does not hold a previous value.
		}
		
		long block = newValue;
		block = (block<<32) | (0xFFFFFFFF&key);
		ht.data[hash&mask] = block;
		return true;
	}
	
   public static void visit(IntHashTable ht, IntHashTableVisitor visitor) {
	   int j = ht.mask+1;
	   while (--j >= 0) {
		   long block = ht.data[j];
		   if (0!=block) {
			   int key = (int)block;
			   int value = (int)(block>>32);			   
			   visitor.visit(key,value);
		   }		   
	   }	   
   }	
   
   public static IntHashTable doubleSize(IntHashTable ht) {
	   IntHashTable newHT = new IntHashTable(ht.bits+1);
	   int j = ht.mask+1;
	   while (--j >= 0) {
		   long block = ht.data[j];
		   if (0!=block) {
			   int key = (int)block;
			   int value = (int)(block>>32);
			   setItem(newHT, key, value);
		   }		   
	   }	   
	   return newHT;
   }
	
}
