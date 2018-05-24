package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.ma.RunningStdDev;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class DataOutputBlobWriter<S extends MessageSchema<S>> extends ChannelWriter {

    protected final Pipe<S> backingPipe;
    final byte[] byteBuffer;
    private final int byteMask;
    private static final Logger logger = LoggerFactory.getLogger(DataOutputBlobWriter.class);
    
    private int startPosition;
    private int activePosition;
    private int lastPosition;
    private int backPosition;
    
 //   private int recTypeIndex = -1;
 //   public static final int REC_TYPE_CHECK = (0xD03)<<20; //value 3331, 12 bits pushed up 
   	
	private final RunningStdDev objectSizeData = new RunningStdDev();

    private boolean structuredWithIndexData = false;
    private final StructuredWriter structuredWriter;
    
    public DataOutputBlobWriter(Pipe<S> p) {
        this.backingPipe = p;
        assert(null!=p) : "requires non null pipe";
        assert(Pipe.isInit(p)): "The pipe must be init before use.";
        this.byteBuffer = Pipe.blob(p);
        this.byteMask = Pipe.blobMask(p);  
        assert(this.byteMask!=0): "mask is "+p.blobMask+" size of blob is "+p.sizeOfBlobRing;
        this.structuredWriter = new StructuredWriter(this);
    }
    
    public void openField() {
    	
        openField(this);
        
    }
	
    @Override
	public boolean reportObjectSizes(Appendable target) {
    	if (RunningStdDev.sampleCount(objectSizeData)>=2) {
			try {		
				target.append("Pipe object writing size data for ")
				      .append(backingPipe.toString())
				      .append("\n");
				RunningStdDev.appendTo(objectSizeData, target);
				target.append("\n");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
    	}
		return true;
	}
	
	
    /**
     * Internal function only used when dependent classes want to add bounds check per method call.
     * @param that
     * @param x
     */
    protected static <T extends MessageSchema<T>> void checkLimit(DataOutputBlobWriter<T> that, int x) {
    	if (that.length()+x > that.backingPipe.maxVarLen ) {
    		throw new RuntimeException("The writer is limited to a maximum length of "+that.backingPipe.maxVarLen
    				                  +". Write less data or declare a larger field. Already wrote "
    				                  +(that.activePosition-that.startPosition)+" at position "+that.activePosition+" attempting to add "+x);
    	}
    }

    public static <T extends MessageSchema<T>> DataOutputBlobWriter<T> openField(final DataOutputBlobWriter<T> writer) {
     	return openFieldAtPosition(writer, Pipe.getWorkingBlobHeadPosition(writer.backingPipe));
    }

	public static <T extends MessageSchema<T>> DataOutputBlobWriter<T> openFieldAtPosition(final DataOutputBlobWriter<T> writer,
																		int workingBlobHeadPosition) {
		assert(workingBlobHeadPosition>=0) : "working head position must not be negative";
		writer.backingPipe.openBlobFieldWrite();
        //NOTE: this method works with both high and low APIs.
		writer.startPosition = writer.activePosition = (writer.byteMask & workingBlobHeadPosition);
        //without any index we can write all the way up to maxVarLen;
		writer.lastPosition = writer.startPosition + writer.backingPipe.maxVarLen;
        //with an index we are limited because room is saved for type data lookup.
        writer.backPosition = writer.startPosition + Pipe.blobIndexBasePosition(writer.backingPipe);
        //this is turned on when callers begin to add index data
        writer.structuredWithIndexData = false;
        DataOutputBlobWriter.setStructType(writer, -1);//clear any previous type
        
        return writer;
	}
    
    public int position() {
    	return activePosition - startPosition;
    }
    
    @Override
    public int remaining() {
    	int result = (lastPosition - activePosition);
    	assert(result <= this.getPipe().maxVarLen);
    	return result;
    }
    
    public void debug() {
        Appendables.appendArray(System.out, '[', backingPipe.blobRing, startPosition, backingPipe.blobMask, ']',  activePosition-startPosition);
    }
    
    public void debugAsUTF8() {
    	Appendable target = System.out;
    	debugAsUTF8(target);
        System.out.println();
    }

	public void debugAsUTF8(Appendable target) {
		Appendables.appendUTF8(target, backingPipe.blobRing, startPosition, activePosition-startPosition, backingPipe.blobMask);
	}
    
	/**
	 * Data written so far is directly copied to the destination writer.
	 * @param writer
	 */
	public void replicate(DataOutputBlobWriter<?> writer) {
		writer.write(backingPipe.blobRing, startPosition, activePosition-startPosition, backingPipe.blobMask);
	}
    
	public void replicate(Appendable target) {
		if (target instanceof DataOutputBlobWriter) {
			replicate((DataOutputBlobWriter<?>)target);
		} else {
			Appendables.appendUTF8(target, backingPipe.blobRing, startPosition, activePosition-startPosition, backingPipe.blobMask);
		}
	}
	
    public static <T extends MessageSchema<T>> boolean tryClearIntBackData(DataOutputBlobWriter<T> writer, int intCount) {	
    	int bytes = (2+intCount)*4;//one for the schema index
    	
    	Pipe.validateVarLength(writer.getPipe(), bytes);
    	
    	int temp = writer.backPosition-bytes;
    	if (temp >= writer.activePosition) {
    		int p = writer.lastPosition;
    		while (--p >= temp) {//clear all
    			writer.byteBuffer[writer.byteMask & p] = (byte)0xFF;
    		}    		
    		writer.backPosition = temp;
    		return true;
    	} else {
    		return false;
    	}    	
    }
    
    public static <T extends MessageSchema<T>> int lastBackPositionOfIndex(DataOutputBlobWriter<T> writer) {
    	return writer.backPosition-writer.startPosition;
    }
    
    public static <T extends MessageSchema<T>> boolean structTypeValidation(DataOutputBlobWriter<T> writer, int value) {
    	int old = getStructType(writer); 
        
    	if (value!=old) {
    		if (old<=0) {
    			writer.structuredWithIndexData = true;    			
    			int base2 = writer.startPosition+Pipe.blobIndexBasePosition(writer.backingPipe);
    			write32(writer.byteBuffer, writer.byteMask, base2, value);
    		} else {
    			throw new UnsupportedOperationException("Type mismatch found "+old+" expected "+value);
    		}
    		
    	}
    	return true;
    }

	public static <T extends MessageSchema<T>> int getStructType(DataOutputBlobWriter<T> writer) {
		final int base = writer.startPosition+Pipe.blobIndexBasePosition(writer.backingPipe);
    	
        return ( ( (       writer.byteBuffer[writer.byteMask & base]) << 24) |
                ( (0xFF & writer.byteBuffer[writer.byteMask & (base+1)]) << 16) |
                ( (0xFF & writer.byteBuffer[writer.byteMask & (base+2)]) << 8) |
                  (0xFF & writer.byteBuffer[writer.byteMask & (base+3)]) );
	
	}
    
    public static <T extends MessageSchema<T>> void setStructType(DataOutputBlobWriter<T> writer, int value) {
    	write32(writer.byteBuffer, writer.byteMask, writer.startPosition+Pipe.blobIndexBasePosition(writer.backingPipe), value);
    }

    public static <T extends MessageSchema<T>> void setIntBackData(DataOutputBlobWriter<T> writer, int value, int pos) {
    	
    	assert(value<=writer.position()) : "wrote "+value+" but all the data is only "+writer.position();
    	
    	assert(pos>=0) : "Can not write beyond the end. Index values must be zero or positive";
    	write32(writer.byteBuffer, writer.byteMask, writer.startPosition+Pipe.blobIndexBasePosition(writer.backingPipe)-(4*(pos+1)), value);       
    }

	public static <T extends MessageSchema<T>> void writeToEndFrom(DataOutputBlobWriter<T> writer, int sizeInBytes, DataInputBlobReader<RawDataSchema> reader) {
	
		DataInputBlobReader.read(reader, 
				    writer.byteBuffer, 
				    writer.startPosition+Pipe.blobIndexBasePosition(writer.backingPipe)-sizeInBytes, 
				    sizeInBytes, 
				    writer.byteMask);
		
	}
    
	@Override
    public int closeHighLevelField(int targetFieldLoc) {
        return closeHighLevelField(this, targetFieldLoc);
    }

    public static <T extends MessageSchema<T>> int closeHighLevelField(DataOutputBlobWriter<T> writer, int targetFieldLoc) {
        //this method will also validate the length was in bound and throw unsupported operation if the pipe was not large enough
        //instead of fail fast as soon as one field goes over we wait to the end and only check once.
        int len = length(writer);
        
        //Appendables.appendUTF8(System.err, writer.byteBuffer, writer.startPosition, len, writer.byteMask);
                
        PipeWriter.writeSpecialBytesPosAndLen(writer.backingPipe, targetFieldLoc, len, writer.startPosition);
        writer.backingPipe.closeBlobFieldWrite();
        
        //Appendables.appendUTF8(System.out, writer.getPipe().blobRing, writer.startPosition, len, writer.getPipe().blobMask);
        
        return len;
    }
    
    @Override
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

    
	 public static <T extends MessageSchema<T>> void commitBackData(DataOutputBlobWriter<T> writer, int structId) {  	
	 	writer.structuredWithIndexData = true;
	 	setStructType(writer, structId);
	 }
    
	 public static <T extends MessageSchema<T>> int closeLowLeveLField(DataOutputBlobWriter<T> writer, int len) {
      
		if (writer.structuredWithIndexData) {
			
			//write this field as length len but move head to the end of maxvarlen
			writer.activePosition = writer.lastPosition;
			Pipe.setBytesWorkingHead(writer.backingPipe, 
					                 writer.activePosition & Pipe.BYTES_WRAP_MASK);			
		    
		} else { 
			//do not keep index just move forward by length size
			Pipe.addAndGetBytesWorkingHeadPosition(writer.backingPipe, len);
		}
		
		assert(writer.startPosition>=0) : "Error bad position of "+writer.startPosition+" length was "+len;
		
        Pipe.addBytePosAndLenSpecial(writer.backingPipe, 
        		                     writer.startPosition, len);
        Pipe.validateVarLength(writer.backingPipe, len);
        writer.backingPipe.closeBlobFieldWrite();
 
        if (writer.structuredWithIndexData) {
        	//set the flag marking this as structed.
        	Pipe.slab(writer.backingPipe)[writer.backingPipe.slabMask & (int)(Pipe.workingHeadPosition(writer.backingPipe)-2)] |= Pipe.STRUCTURED_POS_MASK;
        }
        
        return len;
	}
	 
	 @Override
    public int length() {
        return length(this);
    }

    @Override
    public int absolutePosition() {
        return activePosition;
    }

    @Override
    public void absolutePosition(int absolutePosition) {
        activePosition = absolutePosition;
    }

    public static <T extends MessageSchema<T>> int length(DataOutputBlobWriter<T> writer) {
       
    	return dif(writer, writer.startPosition, writer.activePosition);

    }

	private static <T extends MessageSchema<T>> int dif(DataOutputBlobWriter<T> writer, int pos1, int pos2) {
		return (pos2>=pos1) ? (pos2-pos1) : (writer.backingPipe.sizeOfBlobRing - (writer.byteMask & pos1))+(pos2 & writer.byteMask);
	}
	
	@Override
    public byte[] toByteArray() {
        byte[] result = new byte[length()];        
        Pipe.copyBytesFromToRing(byteBuffer, startPosition, byteMask, result, 0, Integer.MAX_VALUE, result.length);
        return result;
    }
 
	@Override
	public void write(Externalizable object) {		
		int pos = activePosition;
		try {
			object.writeExternal(this);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		assert(Pipe.validateVarLength(this.backingPipe, length(this)));
		assert(collectObjectSizeData(activePosition-pos));		
	}	

	private boolean collectObjectSizeData(int size) {
		//collect all these data points to make a better decision about this pipe length.	
		RunningStdDev.sample(objectSizeData, size);	
		return true;
	}
	
	@Override
    public void writeObject(Object object) {
		    int pos = activePosition;
		    try {
		    	//logger.info("creating new output stream");
	           	ObjectOutputStream oos = new ObjectOutputStream(this); //writes stream header
	           	//logger.info("write the object");
	           	oos.writeObject(object);            
	           	//logger.info("flush");
	            oos.flush();
	            //logger.info("done");
		    } catch (IOException e) {
		    	throw new RuntimeException(e);
		    }
            assert(Pipe.validateVarLength(this.backingPipe, length(this)));
            assert(collectObjectSizeData(activePosition-pos));	
            
    }
    
    @Override
    public void write(int b) {
        byteBuffer[byteMask & activePosition++] = (byte)b;
    }

    @Override
    public void write(byte[] b) {
    	DataOutputBlobWriter.write(this, b, 0, b.length);    	
    }

    @Override
    public void write(byte[] b, int off, int len) {
    	DataOutputBlobWriter.write(this, b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) {
        byteBuffer[byteMask & activePosition++] = (byte) (v ? 1 : 0);
    }

    @Override
    public void writeBooleanNull() {
        byteBuffer[byteMask & activePosition++] = (byte) -1;
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
    	if (null!=s) {
    		activePosition = writeUTF(this, s, s.length(), byteMask, byteBuffer, activePosition);
    	} else {
    		writeShort(-1);
    	}
    	
    }
    
	@Override
    public void writeUTF8Text(CharSequence s) {
	    if (null!=s) {
			encodeAsUTF8(this,s);
		} else {
			writeShort(-1);
		}
    }

	@Override
    public void writeUTF8Text(CharSequence s, int pos, int len) {
    	encodeAsUTF8(this,s,pos,len);
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

    public static int write16(byte[] buf, int mask, int pos, int v) {
        buf[mask & pos++] = (byte)(v >>> 8);
        buf[mask & pos++] = (byte) v;
        return pos;
    }    
    
    public static int write32(byte[] buf, int mask, int pos, int v) {
        buf[mask & pos++] = (byte)(v >>> 24);
        buf[mask & pos++] = (byte)(v >>> 16);
        buf[mask & pos++] = (byte)(v >>> 8);
        buf[mask & pos++] = (byte) v;
        return pos;
    }
    
    public static int write64(byte[] buf, int mask, int pos, long v) {
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
    
	@Override
    public void writeUTF(CharSequence s) {
		if (null!=s) {
			activePosition = writeUTF(this, s, s.length(), byteMask, byteBuffer, activePosition);
		} else {
    		writeShort(-1);
    	}		
	}    
    
	@Override
    public void writeASCII(CharSequence s) {
		
        byte[] localBuf = byteBuffer;
        int mask = byteMask;
        int pos = activePosition;
        if (null!=s) {
	        int len = s.length();        
	        for (int i = 0; i < len; i++) {
	            localBuf[mask & pos++] = (byte)s.charAt(i);
	        }
        }
        localBuf[mask & pos++] = 0;//terminator
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
    
	@Deprecated
    public void writeByteArray(byte[] bytes) {
        activePosition = writeByteArray(this, bytes, bytes.length, byteBuffer, byteMask, activePosition);
    }
    
    private static <T extends MessageSchema<T>> int writeByteArray(DataOutputBlobWriter<T> writer, byte[] bytes, int len, byte[] bufLocal, int mask, int pos) {
    	pos = writePackedInt(bufLocal, mask, pos, len);
        Pipe.copyBytesFromArrayToRing(bytes, 0, writer.byteBuffer, pos, writer.byteMask, len); 
		return pos+len;
    }

    @Deprecated
    public void writeCharArray(char[] chars) {
        activePosition = writeCharArray(chars, chars.length, byteBuffer, byteMask, activePosition);
    }

    private int writeCharArray(char[] chars, int len, byte[] bufLocal, int mask, int pos) {
    	pos = writePackedInt(bufLocal, mask, pos, len);    	
        for(int i=0;i<len;i++) {
            pos = write16(bufLocal, mask, pos, (int) chars[i]);
        }
        return pos;
    }

    @Deprecated
    public void writeIntArray(int[] ints) {
        activePosition = writeIntArray(ints, ints.length, byteBuffer, byteMask, activePosition);
    }

    private int writeIntArray(int[] ints, int len, byte[] bufLocal, int mask, int pos) {
    	pos = writePackedInt(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = write32(bufLocal, mask, pos, ints[i]);
        }
        return pos;
    }

    @Deprecated
    public void writeLongArray(long[] longs) {
        activePosition = writeLongArray(longs, longs.length, byteBuffer, byteMask, activePosition);
    }

    private int writeLongArray(long[] longs, int len, byte[] bufLocal, int mask, int pos) {
    	pos = writePackedInt(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = write64(bufLocal, mask, pos, longs[i]);
        }
        return pos;
    }

    @Deprecated
    public void writeDoubleArray(double[] doubles) {
        activePosition = writeDoubleArray(doubles, doubles.length, byteBuffer, byteMask, activePosition);
    }

    private int writeDoubleArray(double[] doubles, int len, byte[] bufLocal, int mask, int pos) {
    	pos = writePackedInt(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = write64(bufLocal, mask, pos, Double.doubleToLongBits(doubles[i]));
        }
        return pos;
    }

    @Deprecated
    public void writeFloatArray(float[] floats) {
        activePosition = writeFloatArray(floats, floats.length, byteBuffer, byteMask, activePosition);
    }

    private int writeFloatArray(float[] floats, int len, byte[] bufLocal, int mask, int pos) {
    	pos = writePackedInt(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = write32(bufLocal, mask, pos, Float.floatToIntBits(floats[i]));
        }
        return pos;
    }

    @Deprecated
    public void writeShortArray(short[] shorts) {
        activePosition = writeShortArray(shorts, shorts.length, byteBuffer, byteMask, activePosition);
    }

    private int writeShortArray(short[] shorts, int len, byte[] bufLocal, int mask, int pos) {
    	pos = writePackedInt(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = write16(bufLocal, mask, pos, shorts[i]);
        }
        return pos;
    }

    @Deprecated
    public void writeBooleanArray(boolean[] booleans) {
        activePosition = writeBooleanArray(booleans, booleans.length, byteBuffer, byteMask, activePosition);
    }

    private int writeBooleanArray(boolean[] booleans, int len, byte[] bufLocal, int mask, int pos) {
    	//writePackedInt(this,value);
    	pos = writePackedInt(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            bufLocal[mask & pos++] = (byte) (booleans[i] ? 1 : 0);
        }
        return pos;
    }

	@Override
    public void writeUTFArray(String[] utfs) {
        activePosition = writeUTFArray(utfs, utfs.length, byteBuffer, byteMask, activePosition);
    }

    private int writeUTFArray(String[] utfs, int len, byte[] bufLocal, int mask, int pos) {
    	pos = writePackedInt(bufLocal, mask, pos, len);
        for(int i=0;i<len;i++) {
            pos = writeUTF(this, utfs[i], utfs[i].length(), mask, bufLocal, pos);
        }
        return pos;
    }    
    
	@Override
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
    
    //////////
    //write run of byte
    //////////
    public static <T extends MessageSchema<T>> void writeBytes(DataOutputBlobWriter<T> writer, byte pattern, int runLength) {
    	int r = runLength;
    	while (--r>=0) {
    		writer.byteBuffer[writer.byteMask & writer.activePosition++] = pattern;
    	}
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
	@Override
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
	@Override
    public final void writePackedLong(long value) {
        writePackedLong(this,value);
    }
	
	@Override
    public final void writePackedNull() {
        writePackedNull(this);
    }
    
	@Override
    public final void writePackedInt(int value) {
        writePackedInt(this,value);
    }
    
	@Override
    public final void writePackedShort(short value) {
        writePackedInt(this,value);
    }
    
	public static final <T extends MessageSchema<T>> void writePackedNull(DataOutputBlobWriter<T> that) {
		that.write(0);
		that.write(0);
		that.write((byte)0x80);  
	}
	
    public static final <T extends MessageSchema<T>> void writePackedLong(DataOutputBlobWriter<T> that, long value) {

    	that.activePosition = writePackedLong(value, that.byteBuffer, that.byteMask, that.activePosition);

    }

	public static int writePackedLong(long value, byte[] byteBuffer, int byteMask, int position) {
		final long mask = (value>>63);         // FFFFF  or 000000
        final long check = (mask^value)-mask;  //absolute value
        final int bit = (int)(check>>>63);     //is this the special value?
        
        //    Result of the special MIN_VALUE after abs logic                     //8000000000000000
        //    In order to get the right result we must pass something larger than //0x4000000000000000L;
       
		return writeLongUnified(value, (check>>>bit)+bit,
        		byteBuffer, 
        		byteMask, 
        		position, (byte)0x7F);
	}

    public static final <T extends MessageSchema<T>> void writePackedInt(DataOutputBlobWriter<T> that, int value) {

    	that.activePosition = writePackedInt(that.byteBuffer, that.byteMask, that.activePosition, value);

    }

	private static <T extends MessageSchema<T>> int writePackedInt(byte[] buf, int bufMask, int pos, int value) {
		int mask = (value>>31);         // FFFFF  or 000000
        int check = (mask^value)-mask;  //absolute value
        int bit = (int)(check>>>31);     //is this the special value?        
        return writeIntUnified(value, (check>>>bit)+bit, buf, bufMask, pos, (byte)0x7F);
		
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
    public ChannelWriter append(CharSequence s) {
	    if (null!=s) {
			encodeAsUTF8(this,s);
		} else {
			writeByte('n');
			writeByte('u');
			writeByte('l');
			writeByte('l');
		}
        return this;
    }

    @Override
    public ChannelWriter append(CharSequence csq, int start, int end) {
        this.activePosition = encodeAsUTF8(this, csq, start, end-start, this.byteMask, this.byteBuffer, this.activePosition);
        return this;
    }

    @Override
    public ChannelWriter append(char c) {
        this.activePosition = Pipe.encodeSingleChar((int)c, this.byteBuffer, this.byteMask, this.activePosition);
        return this;
    }

	public Pipe<S> getPipe() {
		return backingPipe;
	}

	@Override
	public void writeRational(long numerator, long denominator) {
		writePackedLong(this,numerator);
		writePackedLong(this,denominator);
	}

	public static void copyBackData(DataOutputBlobWriter that, 
			                        byte[] backing, int start, 
			                        int copyLen, int byteMask2) {

		//Appendables.appendArray(System.out.append("To be copied"),  backing, start, byteMask2, copyLen).append("\n");
				
		//method must be redone to only copy what we need.
		Pipe.copyBytesFromToRing(backing, start, byteMask2, 
                that.byteBuffer, 
                that.startPosition + 
                //Pipe.blobIndexBasePosition(that.backingPipe)
                that.backingPipe.maxVarLen    -    copyLen,
                that.byteMask, 
                copyLen);

       that.structuredWithIndexData = true; //no need to set struct since we copied it over

//		Appendables.appendArray(System.out.append("  After copy"),  
//				that.byteBuffer,
//				 that.startPosition +that.backingPipe.maxVarLen-copyLen,
//				 that.byteMask,
//				 copyLen).append("\n");
		 
	   
	   
	}

	public long startsWith(TrieParserReader reader, TrieParser tp) {
		
		return TrieParserReader.query(reader, tp, byteBuffer, startPosition,
				               activePosition - startPosition, byteMask);

	}

	@Override
	public StructuredWriter structured() {
		assert(structuredWriter!=null) : 
			"this pipe was not initialized to support structures, call Pipe.typeData(pipe, registry); first";
		return structuredWriter;
	}

	@Override
	public void writeDecimal(long m, byte e) {
		writeByte(e);
		writePackedLong(m);	
	}

	@Override
	public void reset() {
		activePosition = startPosition;
	}
    
}
