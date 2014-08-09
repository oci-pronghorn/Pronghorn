package com.ociweb.jfast.stream;

import com.ociweb.jfast.loader.FieldReferenceOffsetManager;
import com.ociweb.jfast.util.Stats;

public class FASTRingBufferConsumer {
    private int messageId;
    private boolean isNewMessage;
    public boolean waiting;
    private long waitingNextStop;
    private long bnmHeadPosCache;
    private int cursor;
    public int activeFragmentDataSize;
    private int[] seqStack;
    private int seqStackHead;
    public long tailCache;
    public final FieldReferenceOffsetManager from;
    
    public final Stats queueFill;
    private Stats latency   = new Stats(100, 50, 0, 100);
    
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
        this.queueFill = new Stats(100, rbMask>>1, 0, rbMask);
        
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

    public int getCursor() {
        return cursor;
    }

    public void setCursor(int cursor) {
        this.cursor = cursor;
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
        consumerData.setCursor(-1);
        consumerData.setSeqStackHead(-1);
        
        consumerData.setMessageId(-1);
        consumerData.setNewMessage(false);
        consumerData.activeFragmentDataSize = (0);
        
    }
}