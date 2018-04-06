package com.ociweb.pronghorn.pipe.util.hash;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Non-Thread safe simple fast hash for long to int mapping.
 * 
 * No set is allowed unless no previous value is found.
 * To change previous value replace must be called.
 * Remove can not be supported.
 * Key must not be zero.
 * 
 * @author Nathan Tippy
 *
 */
public class LongHashSet { 

	private final static Logger logger = LoggerFactory.getLogger(LongHashSet.class);
	private final int mask;
	
	private final long[] keys;
		
	private int space;
	
	public LongHashSet(int bits) {
		int size = 1<<bits;
		mask = size-1;
		space = mask; //this is 1 less by design		
		keys = new long[size];		
	}

	public static void clear(LongHashSet source) {
		source.space = source.mask;
		Arrays.fill(source.keys, 0);
	}
	
	public static LongHashSet doubleClone(LongHashSet source) {
		
		final LongHashSet result = new LongHashSet(Integer.bitCount(source.mask)*2);
		
		LongHashSetVisitor visitor = new LongHashSetVisitor(){
			@Override
			public void visit(long key) {
				LongHashSet.setItem(result, key);
			}
		};
		LongHashSet.visit(source, visitor);
		
		return result;
	}
		
	public static boolean setItem(LongHashSet ht, long key)
	{
		if (0==key || 0==ht.space) { 
			return false;
		}
		
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
		
		ht.space--;//gives up 1 spot as a stopper for get.
		
		return true;
	}

	public static boolean isFull(LongHashSet ht) {
		return 0==ht.space;
	}
	

	public static boolean isEmpty(LongHashSet ht) {
		return ht.space == ht.mask;
	}	
		    
	public static boolean hasItem(LongHashSet ht, long key) {

		int mask = ht.mask;
		long[] localKeys = ht.keys;
		
		int hash = MurmurHash.hash64finalizer(key);
		
		long keyAtIdx = localKeys[hash&mask];
		while (keyAtIdx != key && keyAtIdx != 0) { 			
			keyAtIdx = localKeys[++hash&mask];
		}
				
		return 0 != keyAtIdx;
	}
	
	
   public static void visit(LongHashSet ht, LongHashSetVisitor visitor) {
	   int j = ht.mask+1;
	   while (--j>=0) {
		   long key = ht.keys[j];
		   if (0!=key) {			   
			   visitor.visit(key);
		   }		   
	   }	   
   }
	
	public static int computeBits(int count) {
		return (int)Math.ceil(Math.log(count)/Math.log(2));
	}

	
}
