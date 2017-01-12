package com.ociweb.pronghorn.pipe;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

public class DataInputBlobReader<S extends MessageSchema>  extends InputStream implements DataInput {

    private final StringBuilder workspace;
    private final Pipe<S> pipe;
    private byte[] backing;
    private final int byteMask;
    
    private int length;
    private int bytesLimit;
    private int position;
    
    private int EOF_MARKER = -1;
    
    public DataInputBlobReader(Pipe<S> pipe) {
    	super();
        this.pipe = pipe;
        this.backing = Pipe.blob(pipe);
        this.byteMask = Pipe.blobMask(pipe); 
        this.workspace = new StringBuilder(64);
        assert(this.backing!=null) : "The pipe must be init before use.";
    }
    
    public void openHighLevelAPIField(int loc) {
        
        this.length    = PipeReader.readBytesLength(pipe, loc);
        this.position  = PipeReader.readBytesPosition(pipe, loc);
        this.backing   = PipeReader.readBytesBackingArray(pipe, loc);        
        this.bytesLimit = pipe.blobMask & (position + length);
        
        assert(Pipe.validatePipeBlobHasDataToRead(pipe, position, length));

    }
    
    public int openLowLevelAPIField() {
        return openLowLevelAPIField(this);
    }
    
    public static int openLowLevelAPIField(DataInputBlobReader that) {
        
        int meta = Pipe.takeRingByteMetaData(that.pipe);
        that.length    = Pipe.takeRingByteLen(that.pipe);
        that.position = Pipe.bytePosition(meta, that.pipe, that.length);
        that.backing   = Pipe.byteBackingArray(meta, that.pipe);               
        that.bytesLimit = that.pipe.blobMask & (that.position + that.length);
        
        assert(Pipe.validatePipeBlobHasDataToRead(that.pipe, that.position, that.length));
        
        return that.length;
    }

    public int accumLowLevelAPIField() {
        
        if (0==this.length) {
            return openLowLevelAPIField();
        } else {        
        
            int meta = Pipe.takeRingByteMetaData(pipe);
            int len = Pipe.takeRingByteLen(pipe);
            
            this.length += len;
            this.bytesLimit = pipe.byteMask & (bytesLimit + len);
            
            return len;
        }
        
    }
    
        
    public boolean hasRemainingBytes() {
        return (byteMask & position) != bytesLimit;
    }

    @Override
    public int available() {        
        return bytesRemaining(this);
    }

    public static int bytesRemaining(DataInputBlobReader<?> that) {
                
        return  that.bytesLimit >= (that.byteMask & that.position) ? that.bytesLimit- (that.byteMask & that.position) : (that.pipe.sizeOfBlobRing- (that.byteMask & that.position))+that.bytesLimit;

    }

    public DataInput nullable() {
        return length<0 ? null : this;
    }
   
    public int position() {
    	return position;
    }
    
    @Override
    public int read(byte[] b) {
        if ((byteMask & position) == bytesLimit) {
            return EOF_MARKER;
        }       
        
        int max = bytesRemaining(this);
        int len = b.length > max? max : b.length;      
        Pipe.copyBytesFromToRing(backing, position, byteMask, b, 0, Integer.MAX_VALUE, len);
        position += b.length;
        return len;
    }
    
    @Override
    public int read(byte[] b, int off, int len) {
        if ((byteMask & position) == bytesLimit) {
            return EOF_MARKER;
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
    public void readFully(byte[] b) {
                
        Pipe.copyBytesFromToRing(backing, position, byteMask, b, 0, Integer.MAX_VALUE, b.length);
        position += b.length;
       
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
        
        Pipe.copyBytesFromToRing(backing, position, byteMask, b, off, Integer.MAX_VALUE, len);
        position += len;
        
    }

    @Override
    public int skipBytes(int n) {
        
        int skipCount = Math.min(n, length-position);
        position += skipCount;
        
        return skipCount;
    }

    @Override
    public boolean readBoolean() {
        return 0!=backing[byteMask & position++];
    }

    @Override
    public byte readByte() {
        return backing[byteMask & position++];
    }

    @Override
    public int readUnsignedByte() {
        return 0xFF & backing[byteMask & position++];
    }
    
    private static <S extends MessageSchema> short read16(byte[] buf, int mask, DataInputBlobReader<S> that) {
        return (short)((       buf[mask & that.position++] << 8) |
                       (0xFF & buf[mask & that.position++])); 
    }    
    
    private static <S extends MessageSchema> int read32(byte[] buf, int mask, DataInputBlobReader<S> that) {        
        return ( ( (       buf[mask & that.position++]) << 24) |
                 ( (0xFF & buf[mask & that.position++]) << 16) |
                 ( (0xFF & buf[mask & that.position++]) << 8) |
                   (0xFF & buf[mask & that.position++]) ); 
    }
    
    private static <S extends MessageSchema> long read64(byte[] buf, int mask, DataInputBlobReader<S> that) {        
        return ( ( (  (long)buf[mask & that.position++]) << 56) |              
                 ( (0xFFl & buf[mask & that.position++]) << 48) |
                 ( (0xFFl & buf[mask & that.position++]) << 40) |
                 ( (0xFFl & buf[mask & that.position++]) << 32) |
                 ( (0xFFl & buf[mask & that.position++]) << 24) |
                 ( (0xFFl & buf[mask & that.position++]) << 16) |
                 ( (0xFFl & buf[mask & that.position++]) << 8) |
                   (0xFFl & buf[mask & that.position++]) ); 
    }

    @Override
    public short readShort() {
        return read16(backing,byteMask,this);
    }

    @Override
    public int readUnsignedShort() {
        return 0xFFFF & read16(backing,byteMask,this);
    }

    @Override
    public char readChar() {
       return (char)read16(backing,byteMask,this);
    }

    @Override
    public int readInt() {
        return read32(backing,byteMask,this);
    }

    @Override
    public long readLong() {
        return read64(backing,byteMask,this);
    }

    @Override
    public float readFloat() {        
        return Float.intBitsToFloat(read32(backing,byteMask,this));
    }

    @Override
    public double readDouble() {
        return Double.longBitsToDouble(read64(backing,byteMask,this));
    }

    @Override
    public int read() {
        return (byteMask & position) != bytesLimit ? backing[byteMask & position++] : EOF_MARKER;//isOpen?0:-1;
    }

    @Override
    public String readLine() {
        
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
    public String readUTF() {
        int length = readShort(); //read first 2 byte for length in bytes to convert.
        
        workspace.setLength(0);
        try {
        	return readUTF(this, length, workspace).toString();
        } catch (Exception e) {
        	throw new RuntimeException(e);
        }
    }
    
    public <A extends Appendable> A readUTF(A target) {
        int length = readShort(); //read first 2 byte for length in bytes to convert.        
        try {
        	return readUTF(this, length, target);
        } catch (Exception e) {
        	throw new RuntimeException(e);
        }
    }

    public static <A extends Appendable> A readUTF(DataInputBlobReader reader, int length, A target) throws IOException {
        long charAndPos = ((long)reader.position)<<32;
        long limit = ((long)reader.position+length)<<32;

        while (charAndPos<limit) {
            charAndPos = Pipe.decodeUTF8Fast(reader.backing, charAndPos, reader.byteMask);
            target.append((char)charAndPos);
        }
        reader.position+=length;
        return target;
    }
        
    public Object readObject()  {
        
        try {
            return new ObjectInputStream(this).readObject();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    ////
    //support method for direct copy
    ////
    public static void read(DataInputBlobReader reader, byte[] b, int off, int len, int mask) {

        int max = bytesRemaining(reader);
        if (len > max) {
            len = max;
        }
        Pipe.copyBytesFromToRing(reader.backing, reader.position, reader.byteMask, b, off, mask, len);
        reader.position += len;

    }
    
    
    
    
    ///////
    //Packed Chars
    //////
    
    public <A extends Appendable> A readPackedChars(A target) throws IOException {
        readPackedChars(this,target);
        return target;
    }
    
    public static <S extends MessageSchema> void readPackedChars(DataInputBlobReader<S> that, Appendable target) throws IOException {
        int length = readPackedInt(that);
        int i = length;
        while (--i>=0) {
            target.append((char) readPackedInt(that));
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //Support for packed values
    //////////////////////////////////////////////////////////////////////////////////
    //Read signed using variable length encoding as defined in FAST 1.1 specification
    //////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Parse a 64 bit signed value 
     */
    public long readPackedLong() {   
            return readPackedLong(this);
    }

    public int readPackedInt() {   
        return readPackedInt(this);
    }
    
    public short readPackedShort() {
        return (short)readPackedInt(this);
    }

    public static <S extends MessageSchema> long readPackedLong(DataInputBlobReader<S> that) {
        byte v = that.backing[that.byteMask & that.position++];
        long accumulator = (~((long)(((v>>6)&1)-1)))&0xFFFFFFFFFFFFFF80l;
        return (v >= 0) ? readPackedLong((accumulator | v) << 7,that.backing,that.byteMask,that) : (accumulator) |(v & 0x7F);
    }

    public static <S extends MessageSchema> int readPackedInt(DataInputBlobReader<S> that) {
        byte v = that.backing[that.byteMask & that.position++];
        int accumulator = (~((int)(((v>>6)&1)-1)))&0xFFFFFF80; 
        return (v >= 0) ? readPackedInt((accumulator | v) << 7,that.backing,that.byteMask,that) : accumulator |(v & 0x7F);
    }
    
    //recursive use of the stack turns out to be a good way to unroll this loop.
    private static <S extends MessageSchema> long readPackedLong(long a, byte[] buf, int mask, DataInputBlobReader<S> that) {
        return readPackedLongB(a, buf, mask, that, buf[mask & that.position++]);
    }

    private static <S extends MessageSchema> long readPackedLongB(long a, byte[] buf, int mask, DataInputBlobReader<S> that, byte v) {
        assert(a!=0 || v!=0) : "malformed data";
        return (v >= 0) ? readPackedLong((a | v) << 7, buf, mask, that) : a | (v & 0x7Fl);
    }
       
    private static <S extends MessageSchema> int readPackedInt(int a, byte[] buf, int mask, DataInputBlobReader<S> that) {
        return readPackedIntB(a, buf, mask, that, buf[mask & that.position++]);
    }

    private static <S extends MessageSchema> int readPackedIntB(int a, byte[] buf, int mask, DataInputBlobReader<S> that, byte v) {
        assert(a!=0 || v!=0) : "malformed data";
        return (v >= 0) ? readPackedInt((a | v) << 7, buf, mask, that) : a | (v & 0x7F);
    }
    
    
    
}
