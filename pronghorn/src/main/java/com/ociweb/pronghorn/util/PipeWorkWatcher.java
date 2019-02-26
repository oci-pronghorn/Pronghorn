package com.ociweb.pronghorn.util;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipePublishListener;

public class PipeWorkWatcher {

	private int groupBits;
	public int groups;
	private int groupMask;	

	public  long[] tailPos;
    
    private int step;
    private int length;
    
    private AtomicLong[] headPos;

    private AtomicLong workFlags = new AtomicLong();
    
    public PipeWorkWatcher() {
    }
    
    public boolean hasWork() {
    	return 0 != workFlags.get();
    }
    
	public void init(Pipe[] inputs) {
		
		if (inputs.length >= 512) {
			   groupBits = 6;  //64 groups absolute max
		} else {			
			   groupBits = 0;
			
		}
		assert(groupBits<=6);//group bits may not be larger since we use long for mask...
		
		groups = 1<<groupBits;
		groupMask = groups-1;	
		
		length = inputs.length;
		tailPos = new long[length];
		Arrays.fill(tailPos, -1);
			
		
		headPos = new AtomicLong[length];
        int i = inputs.length;
        step = (int)Math.ceil(i/(float)groups);
        if (step<=1) {
        	step = i+1;
        }
        
        
        while (--i >= 0) {
        	headPos[i] = new AtomicLong();        	
        	tailPos[i] = -1;        	
        	
        	final int h = i;
        	final int g = i/step;
        	PipePublishListener listener = new PipePublishListener() {
	    		@Override
	    		public void published(long workingHeadPosition) {		    			
	    			
	    			headPos[h].set(workingHeadPosition);//as long as this number has not moved we have no work.	
	    			
	    			if (workingHeadPosition>tailPos[h]) {
	    				
	    				boolean ok = true;
	    				do {
	    					long old = workFlags.get();
	    					ok = workFlags.compareAndSet(old, old | (1L<<g));
	    					
	    				} while(!ok);
	    				
	    			}
	    		}
	        };
	        
        	Pipe.addPubListener(inputs[i], listener);
        }
        
	}

	public static int getStartIdx(PipeWorkWatcher pww, int g) {
		return g*pww.step;
	}
	
	public static int getLimitIdx(PipeWorkWatcher pww, int g) {
		return Math.min((g+1)*pww.step, pww.length);
	}

		
	public static void setTailPos(PipeWorkWatcher pww, int i, long tailPos) {
		pww.tailPos[i] = tailPos;	
		
	}
	

	public static boolean scan(PipeWorkWatcher pww, int g) {
		
		boolean b = 0!=(pww.workFlags.get()&(1<<g)); 
				//pww.scan[g].get(); 
		if (!b) {
			return b;
		} else {
			
			int s = getStartIdx(pww, g);
			int l = getLimitIdx(pww, g);
			boolean doScan = false;
			for(int i = s; i<l; i++) {
				if (pww.headPos[i].get() > pww.tailPos[i]) {
					doScan=true;
					break;
				}
			}
			
			//pww.scan[g].set(doScan);
			
			if (!doScan) {
				boolean ok = true;
				do {
					long old = pww.workFlags.get();
					ok = pww.workFlags.compareAndSet(old, old &(~(1L<<g)));
					
				} while(!ok);
				
				
				//pww.workFlags.set(pww.workFlags.get()&(~(1<<g)));
			}
			
			return doScan;
		} 
	}

    
}
