package com.ociweb.pronghorn.pipe;

public class PendingReleaseData {
    
    private final int pendingReleaseSize;
    private final int pendingReleaseMask;
    private int pendingReleaseHead;
    private int pendingReleaseTail;
    private int pendingReleaseCount;
    private final int[] pendingBlobReleaseRing;
    private final long[] pendingSlabReleaseRing;
    private final int[] pendingLength;

    public PendingReleaseData(int maxFragsInPipe) {
        pendingReleaseSize = 1 << (int)Math.ceil(Math.log(maxFragsInPipe)/Math.log(2));
        pendingReleaseMask = pendingReleaseSize-1;
        pendingBlobReleaseRing = new int[pendingReleaseSize];
        pendingSlabReleaseRing = new long[pendingReleaseSize];
        pendingLength = new int[pendingReleaseSize];
        
    }

    public static void appendPendingReadRelease(PendingReleaseData that, long slabTail, int blobTail, int fragBytesLen) {
        int idx = that.pendingReleaseMask & that.pendingReleaseHead++;
        assert(that.pendingReleaseHead-that.pendingReleaseTail<=that.pendingLength.length);
        that.pendingBlobReleaseRing[idx] = blobTail;
        that.pendingSlabReleaseRing[idx] = slabTail;
        that.pendingLength[idx] = fragBytesLen;
        that.pendingReleaseCount++;
        assert(that.pendingReleaseCount<=that.pendingLength.length);
    }
    
    public static <S extends MessageSchema> int pendingReleaseCount(PendingReleaseData that) {
    	return that.pendingReleaseCount;
    }
    
    public static <S extends MessageSchema> int pendingReleaseByteCount(PendingReleaseData that) {
    	    
    	int total = 0;
    	int t = that.pendingReleaseTail;
    	int c = that.pendingReleaseCount;
    	while (--c>=0) {    		
    		int idx = that.pendingReleaseMask & t;    		
    		total += that.pendingLength[idx];    		
    		t++;
    	}
    	return total;
    }
    
    
    public static <S extends MessageSchema> void releasePendingReadRelease(PendingReleaseData that, Pipe<S> pipe) {
        if (that.pendingReleaseCount>0) {
            int idx = that.pendingReleaseMask & that.pendingReleaseTail++;
            Pipe.releaseBatchedReads(pipe, 
                                             that.pendingBlobReleaseRing[idx], 
                                             that.pendingSlabReleaseRing[idx]);
            that.pendingReleaseCount--;
        }
    }

    public static <S extends MessageSchema> void releaseAllPendingReadRelease(PendingReleaseData that, Pipe<S> pipe) {
    	
    	if (pipe.batchReleaseCountDownInit<=0) {
    		nonBatchedReleaseAll(that, pipe);
    	} else { 
	        batchedReleaseAll(that, pipe);
    	}
    }

	private static <S extends MessageSchema> void batchedReleaseAll(PendingReleaseData that, Pipe<S> pipe) {
		while (that.pendingReleaseCount>0) {
		    int idx = that.pendingReleaseMask & that.pendingReleaseTail++;
			Pipe.releaseBatchedReads(pipe, 
					 that.pendingBlobReleaseRing[idx], 
					 that.pendingSlabReleaseRing[idx]);

		    that.pendingReleaseCount--;
		}
	}

	private static <S extends MessageSchema> void nonBatchedReleaseAll(PendingReleaseData that, Pipe<S> pipe) {
		//simple case when we do not batch.
		int c = that.pendingReleaseCount;
		if (c>0) {
			that.pendingReleaseTail+=c;	    		
			int idx = that.pendingReleaseMask & (that.pendingReleaseTail-1);	    		 
			Pipe.setBytesTail(pipe, that.pendingBlobReleaseRing[idx]);
			pipe.slabRingTail.tailPos.lazySet(that.pendingSlabReleaseRing[idx]);
			that.pendingReleaseCount = 0;
		}
	}
    
    //releases as the bytes are consumed, this can be called as many times as needed.
    public static <S extends MessageSchema> void releasePendingAsReadRelease(PendingReleaseData that, Pipe<S> pipe, int consumed) {

    	assert(consumed<=Pipe.releasePendingByteCount(pipe)) : "requested more to released than we have pending";
    	
        int idx=0;
        final int mask = that.pendingReleaseMask;
        final int[] pendingLength2 = that.pendingLength;
   
        int rc = that.pendingReleaseCount;
        int rt = that.pendingReleaseTail;
		while (rc>0 && (consumed>0 || pendingLength2[mask & rt]<=0) ) {
            
            idx = mask & rt;
            
            int tmp = pendingLength2[idx];
            if (tmp>consumed) {
            	pendingLength2[idx] = tmp-consumed;
            	that.pendingReleaseCount=rc;
            	that.pendingReleaseTail=rt;
                return;
            }            

            if (tmp>0) {
            	consumed -= tmp;
            }
            
            pendingLength2[idx]=0;
            
            
            Pipe.releaseBatchedReads(pipe, 
                                             that.pendingBlobReleaseRing[idx], 
                                             that.pendingSlabReleaseRing[idx]);
            rc--;
            rt++;
        }
		assert(0==consumed || 0!=rc) : "remaining bytes "+consumed+" but pending fragments "+rc;
		
		that.pendingReleaseCount=rc;
		that.pendingReleaseTail=rt;
    }
    
}