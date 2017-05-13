package com.ociweb.pronghorn.pipe;

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ByteConsumer;

public class DataOutputBlobWriter<S extends MessageSchema<S>> extends OutputStream implements DataOutput, Appendable, ByteConsumer {

    private final Pipe<S> backingPipe;
    private final byte[] byteBuffer;
    private final int byteMask;
    private static final Logger logger = LoggerFactory.getLogger(DataOutputBlobWriter.class);
    
    private int startPosition;
    private int activePosition;
    private int lastPosition;
    private int backPosition;
    
    public DataOutputBlobWriter(Pipe<S> p) {
        this.backingPipe = p;
        assert(null!=p) : "requires non null pipe";
        assert(Pipe.isInit(p)): "The pipe must be init before use.";
        this.byteBuffer = Pipe.blob(p);
        this.byteMask = Pipe.blobMask(p);  
        assert(this.byteMask!=0): "mask is "+p.blobMask+" size of blob is "+p.sizeOfBlobRing;
    }
    
    public void openField() {
    	
        openField(this);
        
    }

    public static <T extends MessageSchema<T>> void openField(DataOutputBlobWriter<T> writer) {
    	openFieldAtPosition(writer, Pipe.getWorkingBlobHeadPosition(writer.backingPipe));
    }

	public static <T extends MessageSchema<T>> void openFieldAtPosition(DataOutputBlobWriter<T> writer,
																		int workingBlobHeadPosition) {
		writer.backingPipe.openBlobFieldWrite();
        //NOTE: this method works with both high and low APIs.
		writer.startPosition = writer.activePosition = workingBlobHeadPosition;
        writer.lastPosition = writer.startPosition + writer.backingPipe.maxVarLen;
        writer.backPosition = writer.lastPosition;
	}
    
    public int position() {
    	return activePosition-startPosition;
    }
    
    
    public void debug() {
        Appendables.appendArray(System.out, '[', backingPipe.blobRing, startPosition, backingPipe.blobMask, ']',  activePosition-startPosition);
    }
    
    public void debugAsUTF8() {
    	Appendables.appendUTF8(System.out, backingPipe.blobRing, startPosition, activePosition-startPosition, backingPipe.blobMask);
        System.out.println();
    }
    
    public static <T extends MessageSchema<T>> boolean tryWriteIntBackData(DataOutputBlobWriter<T> writer, int value) {	
    	    	
    	int totalBytesWritten = dif(writer, writer.startPosition, writer.activePosition);
    	int totalBytesIndexed = 4+dif(writer, writer.backPosition, writer.lastPosition);
    	
    	if (totalBytesWritten+totalBytesIndexed < writer.getPipe().maxVarLen) {
     		writer.backPosition-=4;
    		write32(writer.byteBuffer, writer.byteMask, writer.backPosition, value);       
    		return true;
    	} else {
    		return false;
    	}

    }
    
    public static <T extends MessageSchema<T>> boolean tryClearIntBackData(DataOutputBlobWriter<T> writer, int intCount) {	
    	int bytes = intCount*4;
    	int temp = writer.backPosition-bytes;
    	if (temp >= writer.activePosition) {
    		int p = writer.activePosition;
    		while (--p >= temp) {//clear all
    			writer.byteBuffer[writer.byteMask & p] = 0;
    		}    		
    		writer.backPosition = temp;
    		return true;
    	} else {
    		return false;
    	}    	
    }
    
    public static <T extends MessageSchema<T>> void setIntBackData(DataOutputBlobWriter<T> writer, int value, int pos) {
    	assert(pos>=0) : "Can not write beyond the end.";
    	logger.trace("writing int to position {}",(writer.lastPosition-(4*pos)));
    	write32(writer.byteBuffer, writer.byteMask, writer.lastPosition-(4*pos), value);       
    }
       
    public static <T extends MessageSchema<T>> void commitBackData(DataOutputBlobWriter<T> writer) {
    	
    	final boolean leaveIndexInPlace = true;
    	if (leaveIndexInPlace) {
    		writer.activePosition = writer.lastPosition;
    	} else {
    		///////////////////
    		//older idea where we moved the index down to make the unused space available
    		//this space could not be used however since we have allocated n blocks for n var fields
    		//////////////////
    		//NOTE: delete this block if this is working as expected with the above logic.
	    	int sourceLen = writer.lastPosition-writer.backPosition;
			Pipe.copyBytesFromToRing(writer.byteBuffer, writer.backPosition, writer.byteMask, writer.byteBuffer, writer.activePosition, writer.byteMask, sourceLen); 
			writer.activePosition+=sourceLen;    	
	    	//can only be done once then the end is clear again
	    	writer.backPosition = writer.lastPosition;
    	}
    }
    
    public int closeHighLevelField(int targetFieldLoc) {
        return closeHighLevelField(this, targetFieldLoc);
    }

    public static <T extends MessageSchema<T>> int closeHighLevelField(DataOutputBlobWriter<T> writer, int targetFieldLoc) {
        //this method will also validate the length was in bound and throw unsupported operation if the pipe was not large enough
        //instead of fail fast as soon as one field goes over we wait to the end and only check once.
        int len = length(writer);
        PipeWriter.writeSpecialBytesPosAndLen(writer.backingPipe, targetFieldLoc, len, writer.startPosition);
        writer.backingPipe.closeBlobFieldWrite();
        return len;
    }
    
    public int closeLowLevelField() {
        return closeLowLevelField(this);
    }
    
    /**
     * Close field and record its length as the number of bytes consumed by the BlobWriter
     * @param writer
     */
    public static <T extends MessageSchema<T>> int closeLowLevelField(DataOutputBlobWriter<T> writer) {
        return closeLowLeveLField(writer, length(writer));
    }
    
    /**
     * Ignore the length of this field and close it consuming all the available blob space for this field.
     * @param writer
     */
    public static <T extends MessageSchema<T>> int closeLowLevelMaxVarLenField(DataOutputBlobWriter<T> writer) {
        return closeLowLeveLField(writer, writer.getPipe().maxVarLen);
    }
    
    
	private static <T extends MessageSchema<T>> int closeLowLeveLField(DataOutputBlobWriter<T> writer, int len) {
		Pipe.addAndGetBytesWorkingHeadPosition(writer.backingPipe, len);
        Pipe.addBytePosAndLenSpecial(writer.backingPipe,writer.startPosition,len);
        
        Pipe.validateVarLength(writer.backingPipe, len);
        writer.backingPipe.closeBlobFieldWrite();
        return len;
	}
 
    public int length() {
        return length(this);
    }

    public static <T extends MessageSchema<T>> int length(DataOutputBlobWriter<T> writer) {
       
    	return dif(writer, writer.startPosition, writer.activePosition);

    }

	private static <T extends MessageSchema<T>> int dif(DataOutputBlobWriter<T> writer, int pos1, int pos2) {
		return (pos2>=pos1) ? (pos2-pos1) : (writer.backingPipe.sizeOfBlobRing - (writer.byteMask & pos1))+(pos2 & writer.byteMask);
	}
    
    public byte[] toByteArray() {
        byte[] result = new byte[length()];        
        Pipe.copyBytesFromToRing(byteBuffer, startPosition, byteMask, result, 0, Integer.MAX_VALUE, result.length);
        return result;
    }
 
    
    public void writeObject(Object object) throws IOException {

           	ObjectOutputStream oos = new ObjectOutputStream(this); //writes stream header
            oos.writeObject(object);            
            oos.flush();
            
            assert(Pipe.validateVarLength(this.backingPipe, length(this)));
            
    }
    
    @Override
    public void write(int b) {
        byteBuffer[byteMask & activePosition++] = (byte)b;
    }

    @Override
    public void write(byte[] b) {
        write(this,b,0,b.length,Integer.MAX_VALUE);
    }

    @Override
    public void write(byte[] b, int off, int len) {
        write(this,b,off,len,Integer.MAX_VALUE);
    }

    @Override
    public void writeBoolean(boolean v) {
        byteBuffer[byteMask & activePosition++] = (byte) (v ? 1 : 0);
    }

    @Override
    public void writeByte(int v) {
        byteBuffer[byteMask & activePosition++] = (byte)v;
    }

    @Override
    public void writeShort(int v) {
        activePosition = write16(byteBuffer, byteMask, activePosition, v); 
    }

    @Override
    public void writeChar(int v) {
        activePosition = write16(byteBuffer, byteMask, activePosition, v); 
    }

    @Override
    public void writeInt(int v) {
        activePosition = write32(byteBuffer, byteMask, activePosition, v); 
    }

    @Override
    public void writeLong(long v) {
        activePosition = write64(byteBuffer, byteMask, activePosition, v);
    }

    @Override
    public void writeFloat(float v) {
        activePosition = write32(byteBuffer, byteMask, activePosition, Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(double v) {
        activePosition = write64(byteBuffer, byteMask, activePosition, Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(String s) {
        byte[] localBuf = byteBuffer;
        int mask = byteMask;
        int pos = activePosition;
        int len = s.length();
        for (int i = 0; i < len; i ++) {
            localBuf[mask & pos++] = (byte) s.charAt(i);
        }
        activePosition = pos;
    }

    @Override
    public void writeChars(String s) {
        byte[] localBuf = byteBuffer;
        int mask = byteMask;
        int pos = activePosition;
        int len = s.length();
        for (int i = 0; i < len; i ++) {
            pos = write16(localBuf, mask, pos, (int) s.charAt(i));
        }
        activePosition = pos;
        
    }


    @Override
    public void writeUTF(String s) {
        activePosition = writeUTF(this, s, s.length(), byteMask, byteBuffer, activePosition);
    }
    
    public void writeUTF8Text(CharSequence s) {
    	encodeAsUTF8(this,s);
    }

    private static <T extends MessageSchema<T>> int writeUTF(DataOutputBlobWriter<T> writer, CharSequence s, int len, int mask, byte[] localBuf, int pos) {
        int origPos = pos;
        pos+=2;

        pos = encodeAsUTF8(writer, s, 0, len, mask, localBuf, pos);

        write16(localBuf,mask,origPos, (pos-origPos)-2); //writes bytes count up front
        return pos;
    }
    
    public static <T extends MessageSchema<T>> void encodeAsUTF8(DataOutputBlobWriter<T> writer, CharSequence s) {
        writer.activePosition = encodeAsUTF8(writer, s, 0, s.length(), writer.byteMask, writer.byteBuffer, writer.activePosition);
    }
    
    public static <T extends MessageSchema<T>> void encodeAsUTF8(DataOutputBlobWriter<T> writer, CharSequence s, int position, int length) {
        writer.activePosition = encodeAsUTF8(writer, s, position, length, writer.byteMask, writer.byteBuffer, writer.activePosition);
    }
    
    public static <T extends MessageSchema<T>> int encodeAsUTF8(DataOutputBlobWriter<T> writer, CharSequence s, int sPos, int sLen, int mask, byte[] localBuf, int pos) {
        while (--sLen >= 0) {
            pos = Pipe.encodeSingleChar((int) s.charAt(sPos++), localBuf, mask, pos);
        }
        return pos;
    }
    
    ///////////
    //end of DataOutput methods
    ////////// 
    
    public void writeStream(DataInputBlobReader<S> input, int length) {
    	activePosition += DataInputBlobReader.read(input, byteBuffer, activePosition, length, byteMask);
    }
    
    public static <T extends MessageSchema<T>, S extends MessageSchema<S>> void writeStream(DataOutputBlobWriter<T> that,  DataInputBlobReader<S> input, int length) {
    	that.activePosition += DataInputBlobReader.read(input, that.byteBuffer, that.activePosition, length, that.byteMask);
    }
        
    /////////////////////
    /////////////////////

    private static int write16(byte[] buf, int mask, int pos, int v) {
        buf[mask & pos++] = (byte)(v >>> 8);
        buf[mask & pos++] = (byte) v;
        return pos;
    }    
    
    private static int write32(byte[] buf, int mask, int pos, int v) {
        buf[mask & pos++] = (byte)(v >>> 24);
        buf[mask & pos++] = (byte)(v >>> 16);
        buf[mask & pos++] = (byte)(v >>> 8);
        buf[mask & pos++] = (byte) v;
        return pos;
    }
    
    private static int write64(byte[] buf, int mask, int pos, long v) {
        buf[mask & pos++] = (byte)(v >>> 56);
        buf[mask & pos++] = (byte)(v >>> 48);
        buf[mask & pos++] = (byte)(v >>> 40);
        buf[mask & pos++] = (byte)(v >>> 32);
        buf[mask & pos++] = (byte)(v >>> 24);
        buf[mask & pos++] = (byte)(v >>> 16);
        buf[mask & pos++] = (byte)(v >>> 8);
        buf[mask & pos++] = (byte) v;
        return pos;
    }
    
    public void writeUTF(CharSequence s) {
        activePosition = writeUTF(this, s, s.length(), byteMask, byteBuffer, activePosition);
    }    
    
    public void writeASCII(CharSequence s) {
        byte[] localBuf = byteBuffer;
        int mask = byteMask;
        int pos = activePosition;
        int len = s.length();        
        for (int i = 0; i < len; i ++) {
            localBuf[mask & pos++] = (byte)s.charAt(i);
        }
        activePosition = pos;
    }
         
	@Override
	public void consume(byte[] backing, int pos, int len, int mask) {
		Pipe.copyBytesFromToRing(backing, pos, mask, byteBuffer, activePosition, byteMask, len);
		activePosition += len;
	}

	@Override
	public void consume(byte value) {
		byteBuffer[byteMask & activePosition++] = value;
	}
    
    public void writeByteArray(byte[] bytes) {
        activePosition = writeByteArray(this, bytes, bytes.length, byteBuffer, byteMask, activePosition);
    }
    
    private static <T extends MessageSchema<T>> int writeByteArray(DataOutputBlobWriter<T> writer, byte[] bytes, int len, byte[] bufLocal, int mask, int pos) {
        pos = write32(bufLocal, mask, pos, len);
        Pipe.copyBytesFromToRing(bytes, 0, Integer.MAX_VALUE, writer.byteBuffer, pos, writer.byteMask, len); 
		return pos+len;
    }

    public void writeCharArray(char[] chars) {
        activePosition = writeCharArray(chars, chars.length, byteBuffer, byteMask, activePosition);
    }

    private int writeCharArray(char[] chars, int len, byte[] bufLocal, int mask, int pos) {
        pos = write32(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = write16(bufLocal, mask, pos, (int) chars[i]);
        }
        return pos;
    }

    public void writeIntArray(int[] ints) {
        activePosition = writeIntArray(ints, ints.length, byteBuffer, byteMask, activePosition);
    }

    private int writeIntArray(int[] ints, int len, byte[] bufLocal, int mask, int pos) {
        pos = write32(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = write32(bufLocal, mask, pos, ints[i]);
        }
        return pos;
    }

    public void writeLongArray(long[] longs) {
        activePosition = writeLongArray(longs, longs.length, byteBuffer, byteMask, activePosition);
    }

    private int writeLongArray(long[] longs, int len, byte[] bufLocal, int mask, int pos) {
        pos = write32(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = write64(bufLocal, mask, pos, longs[i]);
        }
        return pos;
    }

    public void writeDoubleArray(double[] doubles) {
        activePosition = writeDoubleArray(doubles, doubles.length, byteBuffer, byteMask, activePosition);
    }

    private int writeDoubleArray(double[] doubles, int len, byte[] bufLocal, int mask, int pos) {
        pos = write32(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = write64(bufLocal, mask, pos, Double.doubleToLongBits(doubles[i]));
        }
        return pos;
    }

    public void writeFloatArray(float[] floats) {
        activePosition = writeFloatArray(floats, floats.length, byteBuffer, byteMask, activePosition);
    }

    private int writeFloatArray(float[] floats, int len, byte[] bufLocal, int mask, int pos) {
        pos = write32(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = write32(bufLocal, mask, pos, Float.floatToIntBits(floats[i]));
        }
        return pos;
    }

    public void writeShortArray(short[] shorts) {
        activePosition = writeShortArray(shorts, shorts.length, byteBuffer, byteMask, activePosition);
    }

    private int writeShortArray(short[] shorts, int len, byte[] bufLocal, int mask, int pos) {
        pos = write32(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = write16(bufLocal, mask, pos, shorts[i]);
        }
        return pos;
    }

    public void writeBooleanArray(boolean[] booleans) {
        activePosition = writeBooleanArray(booleans, booleans.length, byteBuffer, byteMask, activePosition);
    }

    private int writeBooleanArray(boolean[] booleans, int len, byte[] bufLocal, int mask, int pos) {
        pos = write32(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            bufLocal[mask & pos++] = (byte) (booleans[i] ? 1 : 0);
        }
        return pos;
    }

    public void writeUTFArray(String[] utfs) {
        activePosition = writeUTFArray(utfs, utfs.length, byteBuffer, byteMask, activePosition);
    }

    private int writeUTFArray(String[] utfs, int len, byte[] bufLocal, int mask, int pos) {
        pos = write32(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = writeUTF(this, utfs[i], utfs[i].length(), mask, bufLocal, pos);
        }
        return pos;
    }    
    
    public void write(byte[] source, int sourceOff, int sourceLen, int sourceMask) {
    	DataOutputBlobWriter.write(this, source, sourceOff, sourceLen, sourceMask);
    }
    
    public static <T extends MessageSchema<T>> void write(DataOutputBlobWriter<T> writer, byte[] source, int sourceOff, int sourceLen, int sourceMask) {
        Pipe.copyBytesFromToRing(source, sourceOff, sourceMask, writer.byteBuffer, writer.activePosition, writer.byteMask, sourceLen); 
        writer.activePosition+=sourceLen;
    }

    public static <T extends MessageSchema<T>> void write(DataOutputBlobWriter<T> writer, byte[] source, int sourceOff, int sourceLen) {
        Pipe.copyBytesFromArrayToRing(source, sourceOff, writer.byteBuffer, writer.activePosition, writer.byteMask, sourceLen); 
        writer.activePosition+=sourceLen;
    }
    
    ////////
    //low level copy from reader to writer
    ///////
    
    public static <T extends MessageSchema<T>> int writeBytes(DataOutputBlobWriter<T> writer, DataInputBlobReader<RawDataSchema> reader, int length) {

        int len = DataInputBlobReader.read(reader, writer.byteBuffer, writer.activePosition, length, writer.byteMask);        
        writer.activePosition+=len;
        return len;
    }
    
    
    ///////////////////////////////////////////////
    ///New idea: Packed char sequences
    ///////////////////////////////////////////////
    
    public final void writePackedString(CharSequence text) {
        writePackedChars(this,text);
    }
    
    public static <T extends MessageSchema<T>> void writePackedChars(DataOutputBlobWriter<T> that, CharSequence s) {
        that.activePosition = writePackedCharsImpl(that, s, s.length(), 0, that.activePosition, that.byteBuffer, that.byteMask);
    }

    private static <T extends MessageSchema<T>> int writePackedCharsImpl(DataOutputBlobWriter<T> that, CharSequence source, int sourceLength, int sourcePos, int targetPos, byte[] target, int targetMask) {     
        targetPos = writeIntUnified(sourceLength, sourceLength, target, targetMask, targetPos, (byte)0x7F);   
        while (sourcePos < sourceLength) {// 7, 14, 21
            int value = (int) 0x7FFF & source.charAt(sourcePos++);
            targetPos = writeIntUnified(value, value, target, targetMask, targetPos, (byte)0x7F);            
        }
        return targetPos;
    } 
    
    public static <T extends MessageSchema<T>> void writePackedChars(DataOutputBlobWriter<T> that, byte[] source, int sourceMask, int sourceLength, int sourcePos) {
        that.activePosition = writePackedCharsImpl(that, source, sourceMask, sourceLength, sourcePos, that.activePosition, that.byteBuffer, that.byteMask);
    }
    
    private static <T extends MessageSchema<T>> int writePackedCharsImpl(DataOutputBlobWriter<T> that, byte[] source, int sourceMask, int sourceLength, int sourcePos, int targetPos, byte[] target, int targetMask) {
        targetPos = writeIntUnified(sourceLength, sourceLength, target, targetMask, targetPos, (byte)0x7F);        
        while (sourcePos < sourceLength) {
            int value = (int) source[sourceMask & sourcePos++];
            targetPos = writeIntUnified(value, value, target, targetMask, targetPos, (byte)0x7F);            
        }
        return targetPos;
    } 
    
    
    ///////////////////////////////////////////////////////////////////////////////////
    //Support for packed values
    //////////////////////////////////////////////////////////////////////////////////
    //Write signed using variable length encoding as defined in FAST 1.1 specification
    //////////////////////////////////////////////////////////////////////////////////
    
    public final void writePackedLong(long value) {
        writePackedLong(this,value);
    }
    
    public final void writePackedInt(int value) {
        writePackedInt(this,value);
    }
    
    public final void writePackedShort(short value) {
        writePackedInt(this,value);
    }
    
    public static final <T extends MessageSchema<T>> void writePackedLong(DataOutputBlobWriter<T> that, long value) {

        long mask = (value>>63);         // FFFFF  or 000000
        long check = (mask^value)-mask;  //absolute value
        int bit = (int)(check>>>63);     //is this the special value?
        
        //    Result of the special MIN_VALUE after abs logic                     //8000000000000000
        //    In order to get the right result we must pass something larger than //0x4000000000000000L;
       
        that.activePosition = writeLongUnified(value, (check>>>bit)+bit, that.byteBuffer, that.byteMask, that.activePosition, (byte)0x7F);

    }

    public static final <T extends MessageSchema<T>> void writePackedInt(DataOutputBlobWriter<T> that, int value) {

        int mask = (value>>31);         // FFFFF  or 000000
        int check = (mask^value)-mask;  //absolute value
        int bit = (int)(check>>>31);     //is this the special value?
        
        that.activePosition = writeIntUnified(value, (check>>>bit)+bit, that.byteBuffer, that.byteMask, that.activePosition, (byte)0x7F);

    }
    
    public static final <T extends MessageSchema<T>> void writePackedShort(DataOutputBlobWriter<T> that, short value) {
        
        int mask = (value>>31);         // FFFFF  or 000000
        int check = (mask^value)-mask;  //absolute value
        int bit = (int)(check>>>31);     //is this the special value?
        
        that.activePosition = writeIntUnified(value, (check>>>bit)+bit, that.byteBuffer, that.byteMask, that.activePosition, (byte)0x7F);

    }
    
    public static final <T extends MessageSchema<T>> void writePackedULong(DataOutputBlobWriter<T> that, long value) {
        assert(value>=0);
        //New branchless implementation we are testing
        that.activePosition = writeLongUnified(value, value, that.byteBuffer, that.byteMask, that.activePosition, (byte)0x7F);
    }
    
    public static final <T extends MessageSchema<T>> void writePackedUInt(DataOutputBlobWriter<T> that, int value) {
        assert(value>=0);
        that.activePosition = writeIntUnified(value, value, that.byteBuffer, that.byteMask, that.activePosition, (byte)0x7F);
    }
    
    public static final <T extends MessageSchema<T>> void writePackedUShort(DataOutputBlobWriter<T> that, short value) {
        assert(value>=0);
        that.activePosition = writeIntUnified(value, value, that.byteBuffer, that.byteMask, that.activePosition, (byte)0x7F);
    }
    
    private static final int writeIntUnified(final int value,final int check, final byte[] buf, final int mask, int pos, final byte low7) {
   
        if (check < 0x0000000000000040) {
        } else {
            if (check < 0x0000000000002000) {
            } else {
                if (check < 0x0000000000100000) {
                } else {
                    if (check < 0x0000000008000000) {
                    } else {                        
                        buf[mask & pos++] = (byte) (((value >>> 28) & low7));
                    }
                    buf[mask & pos++] = (byte) (((value >>> 21) & low7));
                }
                buf[mask & pos++] = (byte) (((value >>> 14) & low7));
            }
            buf[mask & pos++] = (byte) (((value >>> 7) & low7));
        }
        buf[mask & pos++] = (byte) (((value & low7) | 0x80));
        return pos;
    }
    

    private static final int writeLongUnified(final long value, final long check, final byte[] buf, final int mask, int pos, final byte low7) {
        //NOTE: do not modify this is designed to work best with Intel branch prediction which favors true
        if (check < 0x0000000000000040) {
        } else {
            if (check < 0x0000000000002000) {
            } else {
                if (check < 0x0000000000100000) {
                } else {
                    if (check < 0x0000000008000000) {
                    } else {
                        if (check < 0x0000000400000000L) {
                        } else {
                            if (check < 0x0000020000000000L) {
                            } else {
                                if (check < 0x0001000000000000L) {
                                } else {
                                    if (check < 0x0080000000000000L) {
                                    } else {
                                        if (check < 0x4000000000000000L) {
                                        } else {
                                            buf[mask & pos++] = (byte)(value >>> 63);
                                        }
                                        buf[mask & pos++] = (byte) (( ((int)(value >>> 56)) & low7));
                                    }
                                    buf[mask & pos++] = (byte) (( ((int)(value >>> 49)) & low7));
                                }
                                buf[mask & pos++] = (byte) (( ((int)(value >>> 42)) & low7));
                            }
                            buf[mask & pos++] =(byte) (( ((int)(value >>> 35)) & low7));
                        }
                        buf[mask & pos++] = (byte) (( ((int)(value >>> 28)) & low7));
                    }
                    buf[mask & pos++] = (byte) (( ((int)(value >>> 21)) & low7));
                }
                buf[mask & pos++] = (byte) (( ((int)(value >>> 14)) & low7));
            }
            buf[mask & pos++] = (byte) (( ((int)(value >>> 7)) & low7));
        }
        buf[mask & pos++] = (byte) (( ((int)(value & low7)) | 0x80));
        return pos;
    }

    @Override
    public Appendable append(CharSequence csq) {
        encodeAsUTF8(this, csq);
        return this;
    }

    @Override
    public Appendable append(CharSequence csq, int start, int end) {
        this.activePosition = encodeAsUTF8(this, csq, start, end-start, this.byteMask, this.byteBuffer, this.activePosition);
        return this;
    }

    @Override
    public Appendable append(char c) {
        this.activePosition = Pipe.encodeSingleChar((int)c, this.byteBuffer, this.byteMask, this.activePosition);
        return this;
    }

	public Pipe<S> getPipe() {
		return backingPipe;
	}

    
}
