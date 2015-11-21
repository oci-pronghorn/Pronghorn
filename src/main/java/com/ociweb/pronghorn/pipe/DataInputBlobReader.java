package com.ociweb.pronghorn.pipe;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

public class DataInputBlobReader<S extends MessageSchema>  extends InputStream implements DataInput {

    private final StringBuilder workspace;
    private final Pipe<S> pipe;
    private byte[] backing;
    private final int byteMask;
    
    private ObjectInputStream ois;
    
    private int length;
    private int charLimit;
    private int position;
    
    public DataInputBlobReader(Pipe<S> pipe) {
        this.pipe = pipe;
        this.backing = Pipe.byteBuffer(pipe);
        this.byteMask = Pipe.blobMask(pipe); 
        this.workspace = new StringBuilder(64);
    }
    
    public void openHighLevelAPIField(int loc) {
        
        this.length    = PipeReader.readBytesLength(pipe, loc);
        this.position  = PipeReader.readBytesPosition(pipe, loc);
        this.backing   = PipeReader.readBytesBackingArray(pipe, loc);        
        this.charLimit = position + length;
        
    }
    
    public void openLowLevelAPIField() {
        
        int meta = Pipe.takeRingByteMetaData(pipe);
        this.length    = Pipe.takeRingByteLen(pipe);
        this.position = Pipe.bytePosition(meta, pipe, this.length);
        this.backing   = Pipe.byteBackingArray(meta, pipe);               
        this.charLimit = position + length;
        
    }
    
    
    public DataInput nullable() {
        return length<0 ? null : this;
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
        return backing[byteMask & position++];
    }

    @Override
    public String readLine() throws IOException {
        
        workspace.setLength(0);        
        if (position < charLimit) {
            char c = (char)read16(backing,byteMask,this);
            while (
                    (position < charLimit) &&  //hard stop for EOF but this is really end of field.
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
        
    public Object readObject() throws IOException, ClassNotFoundException  {
        
        if (null==ois) {
            ois = new ObjectInputStream(this);
        }            
        //do we need to reset this before use?
        return ois.readObject();
    }

    
    ///////////////////////////////////////////////////////////////////////////////////
    //Support for packed values
    //////////////////////////////////////////////////////////////////////////////////
    //Read signed using variable length encoding as defined in FAST 1.1 specification
    //////////////////////////////////////////////////////////////////////////////////
    
    /**
     * Parse a 64 bit signed value 
     * 
     * @param reader
     * @return
     */
    public long readPackedLong() {   
            byte v = backing[byteMask & position++];
            long accumulator = (~((long)(((v>>6)&1)-1)))&0xFFFFFFFFFFFFFF80l; 
            return (v < 0) ? accumulator |(v & 0x7F) : readPackedLong((accumulator | v) << 7,backing,byteMask,this);
    }
    //TODO: may want to add an assert to ensure the position did not move further than we expected.
    public int readPackedInt() {   
        byte v = backing[byteMask & position++];
        int accumulator = (~((int)(((v>>6)&1)-1)))&0xFFFFFF80; 
        return (v < 0) ? accumulator |(v & 0x7F) : readPackedInt((accumulator | v) << 7,backing,byteMask,this);
    }

    public short readPackedShort() {
        return (short)readPackedInt();
    }
    
    
    
    //recursive use of the stack turns out to be a good way to unroll this loop.
    private static <S extends MessageSchema> long readPackedLong(long a, byte[] buf, int mask, DataInputBlobReader<S> that) {
        byte v = buf[mask & that.position++];
        return (v<0) ? a | (v & 0x7Fl) : readPackedLong((a | v) << 7, buf, mask, that);
    }
   
    private static <S extends MessageSchema> int readPackedInt(int a, byte[] buf, int mask, DataInputBlobReader<S> that) {
        byte v = buf[mask & that.position++];
        return (v<0) ? a | (v & 0x7F) : readPackedInt((a | v) << 7, buf, mask, that);
    }
    
    
    
}
