package com.ociweb.pronghorn.pipe.util.hash;


/**
 * Lower bound range limited value hash table 
 * 
 * This hash table is used for direct index into values found in a pipe.
 * When the lower bound is raised then values are no longer found in the pipe for use.
 * 
 * @author Nathan Tippy
 *
 */
public class PipeHashTable { 

	private final int mask;
	
	private final long[] keys;
	private final long[] values;
	
	private int space;
	
	private long lowerBounds;
	
	public PipeHashTable(int bits) {
		int size = 1<<bits;
		mask = size-1;
		space = mask; //this is 1 less by design
		
		keys = new long[size];
		values = new long[size];

	}
	
	public static void setLowerBounds(PipeHashTable ht, long value) {
	    ht.lowerBounds = value;
	}
		
	public static boolean setItem(PipeHashTable ht, long key, long value)
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
	
    public static boolean replaceItem(PipeHashTable ht, long key, long newValue) {

        int mask = ht.mask;
        int hash = MurmurHash.hash64finalizer(key);
        
        long keyAtIdx = ht.keys[hash&mask];
        while (keyAtIdx != key && keyAtIdx != 0) {          
            keyAtIdx = ht.keys[++hash&mask];
        }
                
        //if this is a new item
        if (0 == keyAtIdx) {
            ht.keys[hash&mask] = key;
            ht.space--;
        }
        
        ht.values[hash&mask] = newValue;
        return true;
    }
	
	public static long getItem(PipeHashTable ht, long key) {

		int mask = ht.mask;
		int hash = MurmurHash.hash64finalizer(key);
		
		long keyAtIdx = ht.keys[hash&mask];
		while (keyAtIdx != key && keyAtIdx != 0) { 			
			keyAtIdx = ht.keys[++hash&mask];
		}

		long value = ht.values[hash&mask];

		//if value is greater than the lower bound then its ok.
		//if value is greater than the lower then the dif will be negative
		//we take the high bit and fill all 64 then and it with the response
		//if the top is zero then we will return zero, eg not found response.
		return value & ((ht.lowerBounds-(1+value))>>63);
		
	}
	    
	public static boolean hasItem(PipeHashTable ht, long key) {

		int mask = ht.mask;		
		int hash = MurmurHash.hash64finalizer(key);
		
		long keyAtIdx = ht.keys[hash&mask];
		while (keyAtIdx != key && keyAtIdx != 0) { 			
			keyAtIdx = ht.keys[++hash&mask];
		}
		long value = ht.values[hash&mask];		
		return 0 != (value&((ht.lowerBounds-(1+value))>>63));
	}
	
	
   public static void visit(PipeHashTable ht, PipeHashTableVisitor visitor) {
	   int j = ht.mask+1;
	   while (--j>=0) {
		   long key = ht.keys[j];
		   if (0!=key) {			
		       long value = ht.values[j];
		       if (value >= ht.lowerBounds) {
		           visitor.visit(key, value);
		       }
		   }		   
	   }	   
   }	
	
}
