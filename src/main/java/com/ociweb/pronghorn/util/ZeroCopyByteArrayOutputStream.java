package com.ociweb.pronghorn.util;

import java.io.ByteArrayOutputStream;

public class ZeroCopyByteArrayOutputStream extends ByteArrayOutputStream {

    
   public ZeroCopyByteArrayOutputStream(int size) {
        super(size);
    }

public byte[] backingArray() {
       return buf;
   }
   
   public int backingArrayCount() {
       return count;
   }
    
}
