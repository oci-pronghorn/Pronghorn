package com.ociweb.pronghorn.util;

import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;

public class Blocker {

    private final IntHashTable table;
    private final long[] untilTimes;
    private int count;
    
    public enum BlockStatus {
        None,
        Blocked,
        Released
    }
    
    public Blocker(int maxUniqueValues) {
        //we add extra padding to the hash table to avoid collisions which improves the speed.
        table = new IntHashTable( 2 + (int) Math.ceil(Math.log(maxUniqueValues)/Math.log(2)) );
        untilTimes = new long[maxUniqueValues];
    }
    
    /**
     * set release until time for a particular id. 
     * will return false if this id is already waiting on a particular time.
     * 
     * @param id
     * @param untilTime
     * @return
     */
    public boolean until(int id, long untilTime) {
        
        int idx = IntHashTable.getItem(table, id);
        if (0==idx) {
            //new idx
            idx = ++count;
            IntHashTable.setItem(table, id, idx);
        }        
        if (0 != untilTimes[idx-1]) {
            return false;//can not set new time the old one has not been cleared
        }
        untilTimes[idx-1] = untilTime;
        return true;
    }
    
    /**
     * Reads and clears status of id.  
     * 
     * Warning this method has side effect.
     * If Released is returned the stored time is also cleared.
     * @param id
     * @param now
     * @return
     */
    public BlockStatus status(int id, long now) {        
        int idx = IntHashTable.getItem(table, id);
        long time = untilTimes[idx-1];
        
        if (0 == time) {
            return BlockStatus.None;
        } else {
            if (time>now) {
                return BlockStatus.Blocked;
            } else {
                untilTimes[idx-1]=0;
                return BlockStatus.Released;
            }
        }
    }
    
}
