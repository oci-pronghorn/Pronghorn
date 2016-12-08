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
        while (that.pendingReleaseCount>0) {
            int idx = that.pendingReleaseMask & that.pendingReleaseTail++;
            Pipe.releaseBatchedReads(pipe, 
                                             that.pendingBlobReleaseRing[idx], 
                                             that.pendingSlabReleaseRing[idx]);
            that.pendingReleaseCount--;
        }
    }
    
    //releases as the bytes are consumed, this can be called as many times as needed.
    public static <S extends MessageSchema> void releasePendingAsReadRelease(PendingReleaseData that, Pipe<S> pipe, int consumed) {

        int idx=0;
        
        int pLen = 0;
        while (that.pendingReleaseCount>0 && (consumed>0 || that.pendingLength[that.pendingReleaseMask & that.pendingReleaseTail]<=0) ) {
            
            idx = that.pendingReleaseMask & that.pendingReleaseTail;
            
            pLen = Math.max(0, that.pendingLength[idx]);
            
            if (pLen>consumed) {
                that.pendingLength[idx] = pLen-consumed;
                return;
            }
            consumed -= pLen;
            that.pendingLength[idx]=0;
            
            Pipe.releaseBatchedReads(pipe, 
                                             that.pendingBlobReleaseRing[idx], 
                                             that.pendingSlabReleaseRing[idx]);
            that.pendingReleaseCount--;
            that.pendingReleaseTail++;
        }
    }
    
}