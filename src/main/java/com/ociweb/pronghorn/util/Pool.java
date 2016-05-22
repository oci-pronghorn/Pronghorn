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
        int temp = -1;
        while (--i>=0) {
            if (1 == locked[i] && key == keys[i]) {
                return members[i];
            } else {
                if (temp<0 && 0 == locked[i]) {
                    temp = i;
                }
            }
        }
        if (temp<0) {
            return null;
        } else {
            locked[temp] = 1;
            keys[temp] = key;
            return members[temp];
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
    
}
