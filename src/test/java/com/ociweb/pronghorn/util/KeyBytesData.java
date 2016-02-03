package com.ociweb.pronghorn.util;

import com.ociweb.pronghorn.pipe.util.hash.MurmurHash;

public class KeyBytesData {

    private byte[] data;
    private int pos;
    private int len;
    
    public KeyBytesData(byte[] data, int pos, int len) {
        this.data = data;
        this.pos = pos;
        this.len = len;
        
        if (len+pos>data.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        
    }

    @Override
    public int hashCode() {     
        return MurmurHash.hash32(data, pos, len, 777);
    }

    @Override
    public boolean equals(Object obj) {
        
        if (obj instanceof KeyBytesData) {
            KeyBytesData that = (KeyBytesData)obj;
            if (that.data != this.data) {
                return false;
            }
            if (that.len != this.len) {
                return false;
            }
            int i = that.len;
            while (--i >= 0) {
                if (that.data[that.pos+i] != this.data[this.pos+i]) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
        
    }
    
    
    
}
