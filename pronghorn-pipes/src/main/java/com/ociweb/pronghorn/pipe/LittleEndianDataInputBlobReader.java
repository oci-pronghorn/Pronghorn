package com.ociweb.pronghorn.pipe;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public class LittleEndianDataInputBlobReader<S extends MessageSchema<S>>  extends InputStream implements DataInput {

    private final StringBuilder workspace;
    private final Pipe<S> pipe;
    private byte[] backing;
    private final int byteMask;
    
    private ObjectInputStream ois;
    
    private int length;
    private int bytesLimit;
    public int position;
    
    public LittleEndianDataInputBlobReader(Pipe<S> pipe) {
        this.pipe = pipe;
        this.backing = Pipe.blob(pipe);
        this.byteMask = Pipe.blobMask(pipe); 
        this.workspace = new StringBuilder(64);
    }
    
    public void openHighLevelAPIField(int loc) {
        
        this.length    = PipeReader.readBytesLength(pipe, loc);
        this.position  = PipeReader.readBytesPosition(pipe, loc);
        this.backing   = PipeReader.readBytesBackingArray(pipe, loc); 
        
        this.bytesLimit = pipe.blobMask & (position + length);
        
    }
    
    public int openLowLevelAPIField() {
        return openLowLevelAPIField(this);
    }

    private static int openLowLevelAPIField(LittleEndianDataInputBlobReader that) {
        int meta = Pipe.takeByteArrayMetaData((Pipe<?>) that.pipe);
        that.length    = Pipe.takeByteArrayLength((Pipe<?>) that.pipe);
        that.position  = Pipe.bytePosition(meta, that.pipe, that.length);
        that.backing   = Pipe.byteBackingArray(meta, that.pipe);      
        
        that.bytesLimit = that.pipe.blobMask & (that.position + that.length);
        return that.length;
    }
    
    public static void openRawRead(LittleEndianDataInputBlobReader reader, int position, int length) {
        reader.position = position;
        reader.backing  = Pipe.blob(reader.pipe);      
        
        reader.length   = length;
        reader.bytesLimit = reader.pipe.blobMask & (position + length);
    }
    
    
    public int accumLowLevelAPIField() {
        return accumLowLevelAPIField(this);
        
    }

    private static int accumLowLevelAPIField(LittleEndianDataInputBlobReader that) {
        if (0==that.length) {
            return openLowLevelAPIField(that);
        } else {        
        
            int meta = Pipe.takeByteArrayMetaData((Pipe<?>) that.pipe);
            int len = Pipe.takeByteArrayLength((Pipe<?>) that.pipe);
            
            that.length += len;
            that.bytesLimit = that.pipe.blobMask & (that.bytesLimit + len);
            
            return len;
        }
    }
    
    public int length() {
        return length<0?0:length;
    }
        
    public boolean hasRemainingBytes() {
        return (byteMask & position) != bytesLimit;
    }

    @Override
    public int available() throws IOException {        
        return bytesRemaining(this);
    }

    private static int bytesRemaining(LittleEndianDataInputBlobReader that) {
                
        return  that.bytesLimit >= (that.byteMask & that.position) ? that.bytesLimit- (that.byteMask & that.position) : (that.pipe.sizeOfBlobRing- (that.byteMask & that.position))+that.bytesLimit;

    }

    public DataInput nullable() {
        return length<0 ? null : this;
    }
   
    @Override
    public int read(byte[] b) throws IOException {
        if ((byteMask & position) == bytesLimit) {
            return -1;
        }       
        
        int max = bytesRemaining(this);
        int len = b.length>max? max : b.length;      
        Pipe.copyBytesFromToRing(backing, position, byteMask, b, 0, Integer.MAX_VALUE, len);
        position += b.length;
        return b.length;
    }
    
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if ((byteMask & position) == bytesLimit) {
            return -1;
        }
        
        int max = bytesRemaining(this);
        if (len > max) {
            len = max;
        }
        
        Pipe.copyBytesFromToRing(backing, position, byteMask, b, off, Integer.MAX_VALUE, len);
        position += len;
        return len;
    }
    
    @Override
    public void readFully(byte[] b) throws IOException {
                
        Pipe.copyBytesFromToRing(backing, position, byteMask, b, 0, Integer.MAX_VALUE, b.length);
        position += b.length;
       
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        
        Pipe.copyBytesFromToRing(backing, position, byteMask, b, off, Integer.MAX_VALUE, len);
        position += len;
        
    }

    @Override
    public int skipBytes(int n) throws IOException {
        
        int skipCount = Math.min(n, length-position);
        position += skipCount;
        
        return skipCount;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return 0!=backing[byteMask & position++];
    }

    @Override
    public byte readByte() throws IOException {
        return backing[byteMask & position++];
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return 0xFF & backing[byteMask & position++];
    }
    
    private static <S extends MessageSchema<S>> short read16(byte[] buf, int mask, LittleEndianDataInputBlobReader<S> that) {
        return (short)(
                        (0xFF & buf[mask & that.position++]) |
                        (       buf[mask & that.position++] << 8)
                ); 
    }    
    
    private static <S extends MessageSchema<S>> int read32(byte[] buf, int mask, LittleEndianDataInputBlobReader<S> that) {        
        return (
                 (0xFF & buf[mask & that.position++]) |
                 ( (0xFF & buf[mask & that.position++]) << 8) |
                 ( (0xFF & buf[mask & that.position++]) << 16) |
                 ( (       buf[mask & that.position++]) << 24)
               ); 
    }
    
    private static <S extends MessageSchema<S>> long read64(byte[] buf, int mask, LittleEndianDataInputBlobReader<S> that) {        
        return (
                (0xFFl & buf[mask & that.position++]) |
                ( (0xFFl & buf[mask & that.position++]) << 8) |
                ( (0xFFl & buf[mask & that.position++]) << 16) |
                ( (0xFFl & buf[mask & that.position++]) << 24) |
                ( (0xFFl & buf[mask & that.position++]) << 32) |
                ( (0xFFl & buf[mask & that.position++]) << 40) |
                ( (0xFFl & buf[mask & that.position++]) << 48) |
                ( (  (long)buf[mask & that.position++]) << 56)              
                   ); 
    }

    @Override
    public short readShort() throws IOException {
        return read16(backing,byteMask,this);
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return 0xFFFF & read16(backing,byteMask,this);
    }

    @Override
    public char readChar() throws IOException {
       return (char)read16(backing,byteMask,this);
    }

    @Override
    public int readInt() throws IOException {
        return read32(backing,byteMask,this);
    }
    
    public int peekInt() throws IOException {
        int result = read32(backing,byteMask,this);
        position -= 4;
        return result;
    }
    

    @Override
    public long readLong() throws IOException {
        return read64(backing,byteMask,this);
    }

    @Override
    public float readFloat() throws IOException {        
        return Float.intBitsToFloat(read32(backing,byteMask,this));
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(read64(backing,byteMask,this));
    }

    @Override
    public int read() throws IOException {
        return (byteMask & position) != bytesLimit ? backing[byteMask & position++] : -1;
    }

    @Override
    public String readLine() throws IOException {
        
        workspace.setLength(0);        
        if ((byteMask & position) != bytesLimit) {
            char c = (char)read16(backing,byteMask,this);
            while (
                    ((byteMask & position) != bytesLimit) &&  //hard stop for EOF but this is really end of field.
                    c != '\n'
                  ) {
                if (c!='\r') {
                    workspace.append(c);            
                    c = (char)read16(backing,byteMask,this);
                }
            }
        }
        return new String(workspace);
    }

    @Override
    public String readUTF() throws IOException {
        workspace.setLength(0);
        
        int length = readShort(); //read first 2 byte for length in bytes to convert.
        long charAndPos = ((long)position)<<32;
        long limit = ((long)position+length)<<32;

        while (charAndPos<limit) {
            charAndPos = Pipe.decodeUTF8Fast(backing, charAndPos, byteMask);
            workspace.append((char)charAndPos);
        }
        return new String(workspace);
    }
        
    public <T> T readObject() throws IOException {
        
        if (null==ois) {
            ois = new ObjectInputStream(this);
        }            
        //do we need to reset this before use?
        try {
            return (T)ois.readObject();
        } catch (ClassNotFoundException e) {
           throw new IOException(e);
        }
    }

    public void readInto(Pipe<?> selectedPipe, int len) {        
        Pipe.addByteArrayWithMask(selectedPipe, byteMask, len, backing, position);
        position += len;
    }
    
    public static void appendNextFieldToReader(LittleEndianDataInputBlobReader reader, Pipe<RawDataSchema> targetPipe) {
        while (Pipe.hasContentToRead(targetPipe) && Pipe.peekInt(targetPipe) >=0) {
            Pipe.takeMsgIdx(targetPipe);
            accumLowLevelAPIField(reader);
            Pipe.readNextWithoutReleasingReadLock(targetPipe);
            Pipe.confirmLowLevelRead(targetPipe, Pipe.sizeOf(targetPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        }
    }

    public static <A extends Appendable> int readUTF(LittleEndianDataInputBlobReader reader, A target, int bytesCount) throws IOException {
        long charAndPos = ((long)reader.position)<<32;
        long limit = ((long)reader.position+bytesCount)<<32;
        int chars = 0;
        while (charAndPos<limit) {
            charAndPos = Pipe.decodeUTF8Fast(reader.backing, charAndPos, reader.byteMask);
            target.append((char)charAndPos);
            chars++;
        }
        reader.position+=bytesCount;
        return chars;
    }

    public static int peekInt(LittleEndianDataInputBlobReader reader, int off) {
        return (
                  (0xFF & reader.backing[reader.byteMask & reader.position+off]) |
                ( (0xFF & reader.backing[reader.byteMask & reader.position+off]) << 8) |
                ( (0xFF & reader.backing[reader.byteMask & reader.position+off]) << 16) |
                ( (       reader.backing[reader.byteMask & reader.position+off]) << 24)
              ); 
    }

 
}
