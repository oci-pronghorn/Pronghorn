package com.ociweb.jfast.stream;

import com.ociweb.jfast.loader.FieldReferenceOffsetManager;
import com.ociweb.jfast.util.Stats;

public class FASTRingBufferConsumer {
    private int messageId;
    private boolean isNewMessage;
    public boolean waiting;
    private long waitingNextStop;
    private long bnmHeadPosCache;
    public int cursor;
    public int activeFragmentDataSize;
    private int[] seqStack;
    private int seqStackHead;
    public long tailCache;
    public final FieldReferenceOffsetManager from;
    
    public final Stats queueFill;
    public final Stats timeBetween;
    //keep the queue fill size for Little's law 
    //MeanResponseTime = MeanNumberInSystem / MeanThroughput
    private long lastTime = -1;
    private final int rateAvgBit = 5;
    private final int rateAvgCntInit = 1<<rateAvgBit;
    private int rateAvgCnt = rateAvgCntInit;
    
    public FASTRingBufferConsumer(int messageId, boolean isNewMessage, boolean waiting, long waitingNextStop,
                                    long bnmHeadPosCache, int cursor, int activeFragmentDataSize, int[] seqStack, int seqStackHead,
                                    long tailCache, FieldReferenceOffsetManager from, int rbMask) {
        this.messageId = messageId;
        this.isNewMessage = isNewMessage;
        this.waiting = waiting;
        this.waitingNextStop = waitingNextStop;
        this.bnmHeadPosCache = bnmHeadPosCache;
        this.cursor = cursor;
        this.activeFragmentDataSize = activeFragmentDataSize;
        this.seqStack = seqStack;
        this.seqStackHead = seqStackHead;
        this.tailCache = tailCache;
        this.from = from;
        this.queueFill  =  new Stats(10000, rbMask>>1, 0, rbMask);
        this.timeBetween = new Stats(10000, 20, 0, 1000000000);
        
    }

    public static void recordRates(FASTRingBufferConsumer ringBufferConsumer, long newTailPos) {
        
        //MeanResponseTime = MeanNumberInSystem / MeanThroughput
                
        if ((--ringBufferConsumer.rateAvgCnt)<0) {
        
            ringBufferConsumer.queueFill.sample(ringBufferConsumer.bnmHeadPosCache - newTailPos);
            long now = System.nanoTime();
            if (ringBufferConsumer.lastTime>0) {
                ringBufferConsumer.timeBetween.sample(now-ringBufferConsumer.lastTime);
            }
            ringBufferConsumer.lastTime = now;
            
            ringBufferConsumer.rateAvgCnt = ringBufferConsumer.rateAvgCntInit;
        }
        
    }
    
    public static long responseTime(FASTRingBufferConsumer ringBufferConsumer) {
        //Latency in ns
        return (ringBufferConsumer.queueFill.valueAtPercent(.5)*ringBufferConsumer.timeBetween.valueAtPercent(.5))>>ringBufferConsumer.rateAvgBit;
        
    }
    
    public int getMessageId() {
        return messageId;
    }

    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    public boolean isNewMessage() {
        return isNewMessage;
    }

    public void setNewMessage(boolean isNewMessage) {
        this.isNewMessage = isNewMessage;
    }


    public long getWaitingNextStop() {
        return waitingNextStop;
    }

    public void setWaitingNextStop(long waitingNextStop) {
        this.waitingNextStop = waitingNextStop;
    }

    public long getBnmHeadPosCache() {
        return bnmHeadPosCache;
    }

    public void setBnmHeadPosCache(long bnmHeadPosCache) {
        this.bnmHeadPosCache = bnmHeadPosCache;
    }

    public int[] getSeqStack() {
        return seqStack;
    }

    public void setSeqStack(int[] seqStack) {
        this.seqStack = seqStack;
    }

    public int getSeqStackHead() {
        return seqStackHead;
    }
    
    public int incSeqStackHead() {
        return ++seqStackHead;
    }

    public void setSeqStackHead(int seqStackHead) {
        this.seqStackHead = seqStackHead;
    }


    public static void reset(FASTRingBufferConsumer consumerData) {
        consumerData.waiting = (false);
        consumerData.setWaitingNextStop(-1);
        consumerData.setBnmHeadPosCache(-1);
        consumerData.tailCache=-1;
        
        /////
        consumerData.cursor = (-1);
        consumerData.setSeqStackHead(-1);
        
        consumerData.setMessageId(-1);
        consumerData.setNewMessage(false);
        consumerData.activeFragmentDataSize = (0);
        
    }

   
}