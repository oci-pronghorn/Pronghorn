package com.ociweb.pronghorn.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.IOException;

public class ZeroCopyByteArrayOutputStream extends ByteArrayOutputStream implements DataOutput {

   //Fixed size ByteArray output stream.
   //Also not thread safe
    
   public ZeroCopyByteArrayOutputStream(int size) {
        super(size);
    }

   public byte[] backingArray() {
       return buf;
   }
   
   public int backingArrayCount() {
       return count;
   }

   public void reset(int pos) {
       count = pos;    
   }
    
   @Override
   public void write(int b) {
       try {
           //the check to ensure length takes forever and ends up redundant since the JVM does it anyway.
           buf[count++] = (byte) b;
       } catch (ArrayIndexOutOfBoundsException aiobe) {
          grow(); 
       }
   }
   
   @Override
   public void write(byte b[], int off, int len) {
       System.arraycopy(b, off, buf, count, len);
       count += len;
   }

   private void grow() {
        throw new UnsupportedOperationException();
   }

   //length measured in shorts stored as a short
   public void setShortLengthAtStart() {
       if ((count&1)!=0) {
           throw new UnsupportedOperationException();//must be an even number of bytes.
       }
       int len = count>>1;        
       buf[0] = (byte)(len>>8); //TODO: this is backwards must fix
       buf[1] = (byte)len;
   }
   
       
    @Override
    public void writeBoolean(boolean v) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void writeByte(int v) throws IOException {
        write(v);
    }
    
    @Override
    public void writeShort(int v) throws IOException {
        try {
            //the check to ensure length takes forever and ends up redundant since the JVM does it anyway.
            int tmp = count;
            byte[] bufLocal = buf;
            bufLocal[tmp++] = (byte)(v >>> 8);
            bufLocal[tmp++] = (byte) v;
            count=tmp;
        } catch (ArrayIndexOutOfBoundsException aiobe) {
           grow(); 
        }
    }
    
    @Override
    public void writeChar(int v) throws IOException {
        writeInt(v);
    }
    
    @Override
    public void writeInt(int v) throws IOException {
        try {
            //the check to ensure length takes forever and ends up redundant since the JVM does it anyway.
            int tmp = count;
            byte[] bufLocal = buf;
            bufLocal[tmp++]   = (byte)(v >>> 24);
            bufLocal[tmp++]   = (byte)(v >>> 16);
            bufLocal[tmp++]   = (byte)(v >>> 8);
            bufLocal[tmp++]   = (byte) v;
            count=tmp;
        } catch (ArrayIndexOutOfBoundsException aiobe) {
           grow(); 
        }
    }
    
    @Override
    public void writeLong(long v) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void writeFloat(float v) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void writeDouble(double v) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void writeBytes(String s) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void writeChars(String s) throws IOException {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void writeUTF(String s) throws IOException {
        throw new UnsupportedOperationException();
    }


       
}
