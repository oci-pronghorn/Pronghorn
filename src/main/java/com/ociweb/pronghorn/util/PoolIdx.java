package com.ociweb.pronghorn.util;

import java.util.Arrays;

public class PoolIdx  {

    private final long[] keys;
    private final byte[] locked;
    private long locksTaken = 0;
    private long locksReleased = 0;
    private Runnable firstUsage;
    private Runnable noLocks;
    
    public PoolIdx(int length) {
        this.keys = new long[length];
        this.locked = new byte[length];
    }
    
    public int length() {
    	return keys.length;
    }
    
    public String toString() {    	
    	return "Keys:"+Arrays.toString(keys)+"\n"+
    	       "Lcks:"+Arrays.toString(locked)+"\n";    	
    }    
    
    public int getIfReserved(long key) {   
    	
        int i = keys.length;
        int idx = -1;
        //linear search for this key. TODO: if member array is 100 or bigger we should consider hashTable
        while (--i>=0) {
            //found and returned member that matches key and was locked
            if (key == keys[i] && 1 == locked[i]) {
                return i;
            } else {
                //this slot was not locked so remember it
                //we may want to use this slot if key is not found.
                if (idx < 0 && 0 == locked[i]) {
                    idx = i;
                }
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
    	
        int i = keys.length;
        int idx = -1;
        //linear search for this key. TODO: if member array is 100 or bigger we should consider hashTable
        while (--i>=0) {
            //found and returned member that matches key and was locked
            if (key == keys[i] && 1 == locked[i]) {
                return i;
            } else {
                //this slot was not locked so remember it
                //we may want to use this slot if key is not found.
                if (idx < 0 && 0 == locked[i]) {
                    idx = i;
                }
            }
        }
        return startNewLock(key, idx);
    }

    private int startNewLock(long key, int idx) {
        if (idx>=0) {
        	if (0==locksTaken && firstUsage!=null) {
        		firstUsage.run();
        	}
        	locksTaken++;
            locked[idx] = 1;
            keys[idx] = key;
            return idx;
        } else {
            return -1;
        }
    }
    
    public void release(long key) {
        int i = keys.length;
        while (--i>=0) {
            if (key==keys[i]) {
            	locksReleased++;
            	//System.err.println("locks tken "+locksTaken+" and released "+locksReleased);
            	if ((locksReleased==locksTaken) && (noLocks!=null)) {
            		noLocks.run();
            	}
                locked[i] = 0;
                return;
            }
        }        
    }
    
    public int locks() {
        return (int)(locksTaken-locksReleased);
    }
    
    
    
}
