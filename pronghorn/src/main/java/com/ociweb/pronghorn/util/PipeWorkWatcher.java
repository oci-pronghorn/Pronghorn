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
    private AtomicBoolean[] scan;
    private AtomicInteger workFlags = new AtomicInteger();
    
    public PipeWorkWatcher() {
    }
    
    public boolean hasWork() {
    	return 0 != workFlags.get();
    }
    
	public void init(Pipe[] inputs) {
		
		if (inputs.length >= 1024) {
			   groupBits = 8; 
		} else {
			if (inputs.length >= 64 ) {
			   groupBits = 4; 
			} else {
			   groupBits = 0;
			}
		}
		
		groups = 1<<groupBits;
		groupMask = groups-1;	
		
		length = inputs.length;
		tailPos = new long[length];
		Arrays.fill(tailPos, -1);
		
		
		
		scan = new AtomicBoolean[groups];
		int j = groups;
		while (--j>=0) {
			scan[j] = new AtomicBoolean();
		}
		
		
		headPos = new AtomicLong[length];
        int i = inputs.length;
        step = Math.max((int)Math.ceil(1f/(float)groups),i);
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
	    				scan[g].set(true);	    			
	    				workFlags.set(workFlags.get()| (1<<g));
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

		
	public static void setTailPos(PipeWorkWatcher pww, int i, int g, long tailPos) {
		pww.tailPos[i] = tailPos;	
		
	}
	

	public static boolean scan(PipeWorkWatcher pww, int g) {
		
		boolean b = pww.scan[g].get(); 
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
			
			pww.scan[g].set(doScan);
			
			if (!doScan) {
				pww.workFlags.set(pww.workFlags.get()&(~(1<<g)));
			}
			
			return doScan;
		} 
	}

    
}
