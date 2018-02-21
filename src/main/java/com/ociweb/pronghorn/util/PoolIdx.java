package com.ociweb.pronghorn.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PoolIdx  {

    private final long[] keys;
    private final byte[] locked;
    private long locksTaken = 0;
    private long locksReleased = 0;
    private Runnable firstUsage;
    private Runnable noLocks;
    private int rollingKey = 0;
    private final static Logger logger = LoggerFactory.getLogger(PoolIdx.class);
    
    public PoolIdx(int length) {
        this.keys = new long[length];
        this.locked = new byte[length];
    }
    
    public int length() {
    	return keys.length;
    }
    
    public String toString() { 
    	StringBuilder builder = new StringBuilder();
    	
    	for(int i = 0;i<keys.length;i++) {
    		
    		builder.append(" I:");
    		Appendables.appendValue(builder, i);
    		
    		builder.append(" K:");
    		Appendables.appendValue(builder, keys[i]);
    		
    		builder.append(" L:");
    		Appendables.appendValue(builder, locked[i]);
    		builder.append("\n");
    	}
    	return builder.toString();
    }    
    
    public int getIfReserved(long key) {   
    	return getIfReserved(this,key);
    }
    
    public static int getIfReserved(PoolIdx that, long key) {   
    	
    	long[] localKeys = that.keys;
    	byte[] localLocked = that.locked;
        int i = localKeys.length;
        int idx = -1;
        //linear search for this key. TODO: if member array is 100 or bigger we should consider hashTable
        while (--i>=0) {
            //found and returned member that matches key and was locked
            if (0 == localLocked[i] || key != localKeys[i] ) {
            	//this slot was not locked so remember it
            	//we may want to use this slot if key is not found.
            	if (idx < 0 && 0 == localLocked[i]) {
            		idx = i;
            	}
            } else {
            	return i;//this is the rare case
            }
        }
        return -1;
    }
    
    public void setFirstUsageCallback(Runnable run) {
    	firstUsage = run;
    }
    
    public void setNoLocksCallback(Runnable run) {
    	noLocks = run;
    }
    
    public int get(long key) {
    	return get(this,key);
    }
    
    public static int get(PoolIdx that, long key) {   
    	
        long[] localKeys = that.keys;
        byte[] localLocked = that.locked;
        
		int j = localKeys.length;
        int idx = -1;
        
        //linear search for this key. TODO: if member array is bigger than 100 we should consider hashTable
        while (--j>=0) {
        	///////////
        	if (--that.rollingKey<0) { //ensure we continue from where we left off
        		that.rollingKey = that.keys.length-1;
        	}
        	int temp = that.rollingKey;
            /////////
            //found and returned member that matches key and was locked
            if (key == localKeys[temp] && 1 == localLocked[temp]) {
                return temp;
            } else {
                //this slot was not locked so remember it
                //we may want to use this slot if key is not found.
                if (idx < 0 && 0 == localLocked[temp]) {
                    idx = temp;
                }
            }
        }
        return startNewLock(that, key, idx);
    }
    

    
    /**
     * 
     * @param key
     * @param isOk filter to ensure that only acceptable values are chosen
     */
    public int get(long key, PoolIdxPredicate isOk) {   
    	
        int j = keys.length;
        int idx = -1;
        
        //linear search for this key. 
        //TODO: if member array is bigger than 100 we should consider hashTable
        while (--j>=0) {
        	///////////
        	if (--rollingKey<0) { //ensure we continue from where we left off
        		rollingKey = keys.length-1;
        	}
        	int temp = rollingKey;
            /////////
        	
        	//found and returned member that matches key and was locked
            if (key == keys[temp] && 1 == locked[temp]) {
            	
                return temp;
            } else {
                //this slot was not locked so remember it
                //we may want to use this slot if key is not found.
                if (idx < 0 && 0 == locked[temp] && isOk.isOk(temp)) {
                    idx = temp;
                }
            }
            
        }
        //////////////////
        rollingKey = idx; //this line allows for round robin behavior vs 
                          //fill up the pipes if this line is not in place.
        /////////////////
        return startNewLock(this, key, idx);
    }

    private int failureCount = 0;
    
    private static int startNewLock(PoolIdx that, long key, int idx) {
        if (idx>=0) {
        	if (0==that.locksTaken && that.firstUsage!=null) {
        		that.firstUsage.run();
        	}
        	that.locksTaken++;
        	that.locked[idx] = 1;
        	that.keys[idx] = key;
            return idx;
        } else {
        	
        	//unable to find a lock, report this.
        	if (Integer.numberOfLeadingZeros(that.failureCount) != Integer.numberOfLeadingZeros(++that.failureCount)) {
        		logger.info("Unable to find free value from the pool, consider modification of the graph/configuration.");
        	}     	
        	
            return -1;
        }
    }
    
    /**
     * 
     * @param key
     * @return the released pool index value
     */
    public int release(long key) {
        int i = keys.length;
        while (--i>=0) {
            if (key==keys[i]) {
            	locksReleased++;
            	//System.err.println("locks tken "+locksTaken+" and released "+locksReleased);
            	if ((locksReleased==locksTaken) && (noLocks!=null)) {
            		noLocks.run();
            	}
                locked[i] = 0;
                return i;
            }
        } 
        return -1;
    }
    
    public int locks() {
        return (int)(locksTaken-locksReleased);
    }
    
    
    
}
