package com.ociweb.pronghorn.util;

public class Blocker {

    private final long[] untilTimes;
    private int itemCount;
    private int blockedCount;
    
    public enum BlockStatus {
        None,
        Blocked,
        Released
    }
    
    public Blocker(int maxUniqueValues) {
        untilTimes = new long[maxUniqueValues];
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
        
        int idx = id+1;
        itemCount = Math.max(itemCount, idx);
        
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
            if (0!=time && time<now && time<=minTime) {                
                    minTime = time;
                    minIdx = j;
            }
        }
        
        if (minIdx<0) {
            return none;            
        } else {
            blockedCount--;                
            local[minIdx] = 0;
            return minIdx;
        }
	}
    
    public void releaseBlocks(long now) {
        while (-1 != nextReleased(now, -1)) {            
        }        
    }
    
    
    public boolean isBlocked(int id) {        
        int item = id+1;
        return (item<1) ? false : 0!=untilTimes[item-1];
    }
    
    public long isBlockedUntil(int id) {        
        int item = id+1;
        return (item<1) ? 0 : (0==untilTimes[item-1]? 0 : untilTimes[item-1]);
    }
    

    //TODO: urgent needs a unit test to cover
    
    /**
     * Returns true if any block will be released in the defined window.
     * 
     */
    public boolean willReleaseInWindow(long limit) {
    	if (0==blockedCount) {
    		return false;
    	}
    	return willReleaseInWindow(limit, untilTimes);
    }

	private boolean willReleaseInWindow(long limit, long[] local) {
		int i = itemCount; //no need to scan above this point;
        while (--i>=0) {
            long t = local[i];
            if (0==t || t>=limit) {
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
