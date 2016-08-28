package com.ociweb.pronghorn.util;

import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;

public class Blocker {

    private final IntHashTable table;
    private final long[] untilTimes;
    private final int[] ids;
    private int itemCount;
    private int blockedCount;
    
    public enum BlockStatus {
        None,
        Blocked,
        Released
    }
    
    public Blocker(int maxUniqueValues) {
        //we add extra padding to the hash table to avoid collisions which improves the speed.
        table = new IntHashTable( 2 + (int) Math.ceil(Math.log(maxUniqueValues)/Math.log(2)) );
        untilTimes = new long[maxUniqueValues];
        ids = new int[maxUniqueValues];
    }
    
    /**
     * set release until time for a particular id. 
     * will return false if this id is already waiting on a particular time.
     * 
     * @param id
     * @param untilTime
     */
    public boolean until(int id, long untilTime) {
        
    	
        assert(untilTime<System.currentTimeMillis()+(60_000*60*24)) : "until value is set too far in the future";
        
        int idx = IntHashTable.getItem(table, id);
        if (0==idx) {
            //new idx
            idx = ++itemCount;
            IntHashTable.setItem(table, id, idx);
            ids[idx-1] = id;
        }        
        
        if (0 != untilTimes[idx-1]) {
            return false;//can not set new time the old one has not been cleared
        }
        untilTimes[idx-1] = untilTime;
        blockedCount++;
        return true;
    }
    
    
    public int nextReleased(long now, int none) {
        //only scan if we know that something is blocked
        if (0==blockedCount) {
            return none;
        }
        return consumeNextReleased(now, none, untilTimes);        
    }

	private int consumeNextReleased(long now, int none, long[] local) {
		int j = itemCount; //no need to scan above this point
		
        long minTime = Long.MAX_VALUE;
        int minIdx = -1;
        //find the next lowest time to release
        while (--j>=0) {
            long time = local[j];
            if (0!=time && time<now) {
                if (time<=minTime) {
                    minTime=time;
                    minIdx = j;
                }
            }
        }
        
        if (minIdx<0) {
            return none;            
        } else {
            blockedCount--;                
            local[minIdx] = 0;
            return ids[minIdx];
        }
	}
    
    public void releaseBlocks(long now) {
        while (-1 != nextReleased(now, -1)) {            
        }        
    }
    
    
    public boolean isBlocked(int id) {        
        int item = IntHashTable.getItem(table, id);
        return (item<1) ? false : 0!=untilTimes[item-1];
    }
    
    public long isBlockedUntil(int id) {        
        int item = IntHashTable.getItem(table, id);
        return (item<1) ? 0 : (0==untilTimes[item-1]? 0 : untilTimes[item-1]);
    }
    

    //TODO: urgent needs a unit test to cover
    
    /**
     * Returns true if any block will be released in the defined window.
     * 
     * @param currentTimeMillis
     * @param msNearWindow
     */
    public boolean willReleaseInWindow(long currentTimeMillis, long msNearWindow) {
    	if (0==blockedCount) {
    		return false;
    	}
    	return willReleaseInWindow(currentTimeMillis, msNearWindow, untilTimes);
    }

	private boolean willReleaseInWindow(long currentTimeMillis, long msNearWindow, long[] local) {
		int i = itemCount; //no need to scan above this point
        long limit = currentTimeMillis+msNearWindow;
        while (--i>=0) {
            long t = local[i];
            if (t<currentTimeMillis || t>=limit) {
            } else {
            	return true;
            }
        }
        return false;
	}

    public long durationToNextRelease(long currentTimeMillis, long defaultValue) {
    	if (0==blockedCount) {
    		return defaultValue;
    	}
        int i = itemCount; //no need to scan above this point
        long[] local = untilTimes;
        long minValue = defaultValue;
        while (--i>=0) {
            long t = local[i];
            if (t>=currentTimeMillis) {
                long duration = t-currentTimeMillis;
                minValue = Math.min(duration, minValue);
            }
        }
        return minValue;
    }
    
    
    
    
}
