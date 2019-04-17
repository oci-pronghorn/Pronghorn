package com.ociweb.pronghorn.util;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PoolIdx  {

    private final long[] keys;
    private final byte[] locked;
    public final int groups;
    private final int step;
    private long locksTaken = 0;
    private long locksReleased = 0;
    private Runnable firstUsage;
    private Runnable noLocks;
    private final static Logger logger = LoggerFactory.getLogger(PoolIdx.class);
    
    public PoolIdx(int length, int groups) {
        this.keys = new long[length];        
        Arrays.fill(keys, -1L);
        this.locked = new byte[length];
        this.groups = groups;
        
        if ((length%groups != 0) || (groups>length)) {
        	throw new UnsupportedOperationException("length must be divisiable by groups value. length:"+length+" groups:"+groups);
        }
        
        this.step = length/groups;
    }
    
    public int length() {
    	return keys.length;
    }
    
    public String toString() { 
    	StringBuilder builder = new StringBuilder();
    	
    	for(int i = 0;i<keys.length;i++) {
    		
    		Appendables.appendValue(builder, " Idx:", i);    		
    		Appendables.appendValue(builder, " Key:", keys[i]);    		
    		Appendables.appendValue(builder, " Lok:", locked[i]);
    		builder.append("\n");
    	}
    	return builder.toString();
    }    
    
    public int getIfReserved(long key) {   
    	return getIfReserved(this, key);
    }
    
    public static int getIfReserved(PoolIdx that, long key) {   
    	
    	long[] localKeys = that.keys;
    	byte[] localLocked = that.locked;
        int i = localKeys.length;
    
        //linear search for this key. TODO: if member array is 100 or bigger we should consider hashTable
        while (--i >= 0) {
            //found and returned member that matches key and was locked
            if (0 == localLocked[i] || key != localKeys[i] ) {
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
    	return that.get(key, PoolIdxPredicate.allOk);       
    }
    
    
    int lastUsedGroup = 0;//must hold this state so each request goes to the next goup.
    /**
     * 
     * @param key
     * @param isOk filter to ensure that only acceptable values are chosen
     */
    public int get(long key, PoolIdxPredicate isOk) {   
    	
        int idx = -1;
        
        int j = step;        
        while (--j>=0) {
        	int g = groups;
        	int gr = lastUsedGroup;
        	while (--g>=0) {
	        	///////////
        		if (--gr<0) {
        			gr = groups-1;
        		}
        		
	        	int temp = (gr*step)+j;
	            /////////
	        	
	        	//found and returned member that matches key and was locked
	            if (1 == locked[temp]) {
	            	if (key == keys[temp]) {
	            		return temp;
	            	}
	            } else {
	            		            	
	            	if (isOk.isOk(temp)) {	            		
	            		if (idx<0) {
	            			//not already set
	            			if (0 == locked[temp]) {
	            				idx = temp; //if unlocked take it
	            				lastUsedGroup = gr;	            				
	            			}
	            		} else {
	            			//already set do we have something better?
	            			if (key == keys[temp]) {
	            				idx = temp; //take this one since we had it before
	            				lastUsedGroup = gr;
	            				break;
	            			} else if (-1 == keys[temp] //never used
 	            					  && keys[idx]!=key) { //and not already assigned to previous selection
	            				idx = temp;
	            				lastUsedGroup = gr;
	            			}          			
	            		}	                
	            	}	            	
	            }
	        }  
        }
        return startNewLock(this, key, idx);
    }
    
    /**
     * 
     * @param key
     * @param fixedGroup group where key will be found for faster lookup
     */
    public int get(long key, int fixedGroup) {   
    	
        int idx = -1;
        
        int base = fixedGroup*step;
        int temp = step+base;    
        
        while (--temp >= base) {        	
        	
	        	//found and returned member that matches key and was locked
        	        	
	        	if (key == keys[temp]) {
	        		if (1 == locked[temp]) {
	        			return temp;
	        		} else {
	        			//unlocked but this was the spot
	        			idx = temp; //take this one since we had it before
        				break;
	        		}
	        	} else {	                        		
            		if (idx<0) {
            			//not already set
            			if (0 == locked[temp]) {
            				idx = temp; //if unlocked take it            				           				
            			}
            		} else {
            			//already set do we have something better?
            			if (-1 == keys[temp] //never used
            					  && keys[idx]!=key) { //and not already assigned to previous selection
            				idx = temp;
            				
            			}          			
            		}
	            }
	          
        }
        return startNewLock(this, key, idx);
    }

    private int failureCount = 0;
    
    public int optimisticGet(long key, int idx) {
    	if (key == keys[idx]) {
    		if (0 == locked[idx]) {
    			return startNewLock(this,key,idx);
    		} else {
    			return idx;//already locked
    		}
    	} else {
    		return -1;
    	}
    	
    }
    
    private static int startNewLock(PoolIdx that, long key, int idx) {
 
        if (idx>=0) {
        	if ((0!=that.locksTaken) || (that.firstUsage==null)) {
            	that.locksTaken++;
            	that.locked[idx] = 1;
            	that.keys[idx] = key;

        	} else {
            	that.locksTaken++;
            	that.locked[idx] = 1;
            	that.keys[idx] = key;
        		
        		that.firstUsage.run();
        	}
            return idx;
        } else {
        	
//        	boolean debug = false;
//        	if (debug) {
//	        	//DO NOT REPORT UNLESS DEBUGGING.
//	        	//unable to find a lock, report this.
//	        	if (Integer.numberOfLeadingZeros(that.failureCount) != Integer.numberOfLeadingZeros(++that.failureCount)) {
//	        		logger.info("Unable to find free value from the pool, consider modification of the graph/configuration.");
//	        	}     	
//        	}
        	
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
    
    /**
     * fast release since we already know where to find this one.
     * also does NOT notify any listeners for no locks since it must be fast.
     * 
     * @param key
     * @param idx
     * @return the released pool index value
     */
    public static int release(final PoolIdx that, final long key, final int idx) {

        if (key==that.keys[idx]) {
        	that.locksReleased++;
        	that.locked[idx] = 0;
            return idx;
        }
        return -1;
    }
    
    public void visitLocked(PoolIdxKeys visitor) {
        int i = keys.length;
        while (--i>=0) {
            if (0 != locked[i]) {            
            	visitor.visit(keys[i]);
            }
        }
    }
    
    public int locks() {
        return (int)(locksTaken-locksReleased);
    }
    
    
    
}
