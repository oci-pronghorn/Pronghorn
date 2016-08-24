package com.ociweb.pronghorn.util;

public class Pool<T>  {

    private final T[] members;
    private final long[] keys;
    private final byte[] locked;
    
    public Pool(T[] members) {
        this.members = members;
        this.keys = new long[members.length];
        this.locked = new byte[members.length];
    }
    
    public T[] members() {
        return members;
    }
    
    public T get(long key) {        
        int i = keys.length;
        int idx = -1;
        //linear search for this key. TODO: if member array is 100 or bigger we should consider hashTable
        while (--i>=0) {
            //found and returned member that matches key and was locked
            if (key == keys[i] && 1 == locked[i]) {
                return members[i];
            } else {
                //this slot was not locked so remember it
                //we may want to use this slot if key is not found.
                if (idx < 0 && 0 == locked[i]) {
                    idx = i;
                }
            }
        }
        return startNewLock(key, idx);
    }

    private T startNewLock(long key, int idx) {
        if (idx>=0) {
            locked[idx] = 1;
            keys[idx] = key;
            return members[idx];
        } else {
            return null;
        }
    }
    
    public void release(long key) {
        int i = keys.length;
        while (--i>=0) {
            if (key==keys[i]) {
                locked[i] = 0;
                return;
            }
        }        
    }
    
    public int locks() {
        int count = 0;
        int j = locked.length;
        while (--j>=0) {
            count += locked[j];
        }
        return count;
    }
    
    
    
}
