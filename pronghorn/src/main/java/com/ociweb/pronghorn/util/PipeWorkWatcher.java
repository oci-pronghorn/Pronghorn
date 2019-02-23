package com.ociweb.pronghorn.util;

import java.util.Arrays;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipePublishListener;

public class PipeWorkWatcher {

	private int groupBits;
	public int groups;
	private int groupMask;	

	public  long[] tailPos;
    private long[] headPos;
    
    private int step;
    private int length;
    
    private boolean[] scan;
    private boolean[] isDirty;
    
    public PipeWorkWatcher() {
    }
    
	public void init(Pipe[] inputs) {
		
		if (inputs.length >= 32) {
			groupBits = 4;
		} else {
			groupBits = 0;
		}
		
		groups = 1<<groupBits;
		groupMask = groups-1;	
		
		length = inputs.length;
		tailPos = new long[length];
		Arrays.fill(tailPos, -1);
		
		headPos = new long[length];
		
		scan = new boolean[groups];
		isDirty = new boolean[groups];
		
        int i = inputs.length;
        step = Math.max((int)Math.ceil(1f/(float)groups),i);
        while (--i >= 0) {
        	tailPos[i] = -1;
        	headPos[i] = -1;   
        	
        	
        	final int h = i;
        	final int g = i/step;
        	PipePublishListener listener = new PipePublishListener() {
	    		@Override
	    		public void published(long workingHeadPosition) {		    			
	    			headPos[h]=workingHeadPosition;//as long as this number has not moved we have no work.		    			
	    			if (workingHeadPosition>tailPos[h]) {
	    				scan[g] = true;
	    				isDirty[g] = true;
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
		pww.isDirty[g] = true;
	}
	

	public static boolean scan(PipeWorkWatcher pww, int g) {
		
		if (!pww.scan[g] || !pww.isDirty[g]) {
			return pww.scan[g];
		} else {
			
			int s = getStartIdx(pww, g);
			int l = getLimitIdx(pww, g);
			boolean doScan = false;
			for(int i = s; i<l; i++) {
				if (pww.headPos[i] > pww.tailPos[i]+1) {
					doScan=true;
					break;
				}
			}
			pww.isDirty[g] = false;
			pww.scan[g] = doScan;
			return doScan;
		} 
	}
    
    
    
}
