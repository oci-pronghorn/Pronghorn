package com.ociweb.pronghorn.ring.util;

import java.util.Arrays;


/**
 * Non-Thread safe simple fast hash for int to int mapping.
 * 
 * No set is allowed unless no previous value is found.
 * To change previous value replace must be called.
 * Remove can not be supported.
 * 
 * key must not be zero.
 * 
 * @author Nathan Tippy
 *
 *  TODO: needs more unit tests urgently.
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
		if (key==0 || ht.memberCount>=ht.mask) { //gives up 1 spot as a stopper for get.
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
		ht.memberCount++;
		
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
