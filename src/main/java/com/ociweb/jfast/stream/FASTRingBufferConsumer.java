package com.ociweb.jfast.stream;

import com.ociweb.jfast.loader.FieldReferenceOffsetManager;

public class FASTRingBufferConsumer {
    private int messageId;
    private boolean isNewMessage;
    private boolean waiting;
    private long waitingNextStop;
    private long bnmHeadPosCache;
    private int cursor;
    private int activeFragmentDataSize;
    private int[] seqStack;
    private int seqStackHead;
    private long tailCache;
    public final FieldReferenceOffsetManager from;

    public FASTRingBufferConsumer(int messageId, boolean isNewMessage, boolean waiting, long waitingNextStop,
                                    long bnmHeadPosCache, int cursor, int activeFragmentDataSize, int[] seqStack, int seqStackHead,
                                    long tailCache, FieldReferenceOffsetManager from) {
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

    public boolean isWaiting() {
        return waiting;
    }

    public void setWaiting(boolean waiting) {
        this.waiting = waiting;
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

    public int getActiveFragmentDataSize() {
        return activeFragmentDataSize;
    }

    public void setActiveFragmentDataSize(int activeFragmentDataSize) {
        this.activeFragmentDataSize = activeFragmentDataSize;
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

    public long getTailCache() {
        return tailCache;
    }

    public void setTailCache(long tailCache) {
        this.tailCache = tailCache;
    }

    public static void reset(FASTRingBufferConsumer consumerData) {
        consumerData.setWaiting(false);
        consumerData.setWaitingNextStop(-1);
        consumerData.setBnmHeadPosCache(-1);
        consumerData.setTailCache(-1);
        
        /////
        consumerData.setCursor(-1);
        consumerData.setSeqStackHead(-1);
        
        consumerData.setMessageId(-1);
        consumerData.setNewMessage(false);
        consumerData.setActiveFragmentDataSize(0);
        
    }
}