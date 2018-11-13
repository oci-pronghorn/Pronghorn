package com.ociweb.pronghorn.pipe;

import java.io.DataInput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.util.IntArrayPool;
import com.ociweb.pronghorn.pipe.util.IntArrayPoolLocal;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.math.Decimal;
import com.ociweb.pronghorn.util.math.DecimalResult;

public class DataInputBlobReader<S extends MessageSchema<S>> extends ChannelReader {

    private final StringBuilder workspace;
    private final Pipe<S> pipe;
    private byte[] backing;
    private final int byteMask;
    private static final Logger logger = LoggerFactory.getLogger(DataInputBlobReader.class);
    
    protected int length;
    private int bytesHighBound;
    protected int bytesLowBound;
    protected int position;
    protected boolean isStructured;
    
    private final StructuredReader structuredReader;
    
    /////////////////////////
    //package protected DimArray methods
    /////////////////////////    
    public static int reserveDimArray(DataInputBlobReader reader, int size, int maxSize) {   	   	
    	return IntArrayPool.lockInstance(IntArrayPoolLocal.get(), size);  	
    }
    public static int[] lookupDimArray(DataInputBlobReader reader, int size, int instance) {
      	return IntArrayPool.getArray(IntArrayPoolLocal.get(), size, instance);  	
    }
    public static void releaseDimArray(DataInputBlobReader reader, int size, int instance) {
    	IntArrayPool.releaseLock(IntArrayPoolLocal.get(), size, instance);
    }
    ////////////////////////////////
    ////////////////////////////////
    
    
    private static TrieParser textToNumberParser;
    protected TrieParserReader reader;
    
    private long mostRecentPacked=-1; //used for asserts;
    
    private int EOF_MARKER = -1;
    
    public DataInputBlobReader(Pipe<S> pipe) {
    	super();
        this.pipe = pipe;
        this.backing = Pipe.blob(pipe);
        this.byteMask = Pipe.blobMask(pipe); 
        this.workspace = new StringBuilder(64);
        assert(this.backing!=null) : "The pipe must be init before use.";
        
        structuredReader = new StructuredReader(this);
    }
        
	public void debug() {
	    
    	Appendables.appendArray(Appendables.appendValue(System.out,  "read at ", bytesLowBound), '[', backing, bytesLowBound, byteMask, ']',  length);

	}
	
	public void debugUTF8() {
	    Appendables.appendUTF8(System.out, backing, bytesLowBound, length, byteMask);
		
	}
	
    public int openHighLevelAPIField(int loc) {
        
    	this.isStructured = PipeReader.isStructured(pipe, loc);    	
        this.length         = Math.max(0, PipeReader.readBytesLength(pipe, loc));
        this.bytesLowBound  = this.position       = PipeReader.readBytesPosition(pipe, loc);
        this.backing        = PipeReader.readBytesBackingArray(pipe, loc); 
        assert(this.backing!=null) : "The pipe must be init before use.";
        this.bytesHighBound = pipe.blobMask & (position + length);
        
        assert(Pipe.validatePipeBlobHasDataToRead(pipe, position, length));
        
        return this.length;
    }
    
    public static int peekHighLevelAPIField(DataInputBlobReader<?> reader, int loc) {
        
    	reader.isStructured   = 0!=(Pipe.STRUCTURED_POS_MASK&PipeReader.peekDataMeta(reader.pipe, loc));
    	reader.length         = PipeReader.peekDataLength(reader.pipe, loc);
    	reader.bytesLowBound  = reader.position = PipeReader.peekDataPosition(reader.pipe, loc);
    	reader.backing        = PipeReader.peekDataBackingArray(reader.pipe, loc); 
    	assert(reader.backing!=null) : "The pipe must be init before use.";
    	reader.bytesHighBound = reader.pipe.blobMask & (reader.position + reader.length);
     
        return reader.length;
    }
    
    
    public int readFromEndLastInt(int negativeIntOffset) {
    	assert(readFromLastInt(this, negativeIntOffset)<=this.length) :
    		  "index position is out of bounds in pipe "+this.getBackingPipe(this).id
    		 +" at idx "+negativeIntOffset
    		 +" bad pos of "+readFromLastInt(this, negativeIntOffset)
    		 +" full length "+this.length;
    	
    	
    	return readFromLastInt(this, negativeIntOffset);
    }

    
    public static boolean structTypeValidation(DataInputBlobReader<?> reader, int structId) {
    	if (getStructType(reader)!=structId) {
    		throw new UnsupportedOperationException("Type mismatch");
    	}
    	
    	return true;
    }
    
    public boolean isStructured() {
    	return isStructured(this);
    }
       
    
    public static boolean isStructured(DataInputBlobReader<?> reader) {
    	return reader.isStructured;
    }
    
    //not as useful as you might think
	public static int getStructType(DataInputBlobReader<?> reader) {
		//must return -1 when structures are not used, in that case we may have 
		//valid data beyond the body of this payload. so we can not fetch the value.
		return (!reader.isStructured) 
				? -1 
			    : StructRegistry.IS_STRUCT_BIT | bigEndianInt((reader.bytesLowBound + Pipe.blobIndexBasePosition(reader.pipe)), reader.byteMask, reader.backing);
		
	}
	
	private static int bigEndianInt(int position, int mask, byte[] back) {
		return ( ( back[mask & position++]) << 24) |
				 ( (0xFF & back[mask & position++]) << 16) |
				 ( (0xFF & back[mask & position++]) << 8) |
				   (0xFF & back[mask & position]);
	}
    
	public static int readFromLastInt(DataInputBlobReader<?> reader, int pos) {
		assert(pos>=0) : "there is no data found at the end";
    	//logger.info("readFromEndLastInt pos {} ",position);    	
    	return bigEndianInt((reader.bytesLowBound + Pipe.blobIndexBasePosition(reader.pipe))-(4*(pos+1)), reader.byteMask, reader.backing);
	}
        
	public boolean readFromEndInto(DataOutputBlobWriter<?> outputStream) {
		assert(isStructured) : "method can only be called on structured readers";
		//WARNING: this method will carry the same exact struct forward to the destination
		
		final int type = getStructType(this);
		//only copy back data if it is found.
		if (type!=-1) {
			//warning this must copy all the way to the very end with maxVarLen
			final int end = (bytesLowBound + pipe.maxVarLen);
			
			final int copyLen = ((Pipe.structRegistry(getBackingPipe(this)).totalSizeOfIndexes(type))*4)+4;//plus the type
			int start = end-copyLen;
			
			DataOutputBlobWriter.copyBackData(outputStream, backing, start, copyLen, byteMask);
			return true;
		}
		return false;
	}
	
    
    public int openLowLevelAPIField() {
        int meta = Pipe.takeByteArrayMetaData(this.pipe);
        
        this.isStructured = (0!=(Pipe.STRUCTURED_POS_MASK&meta));
                
		this.length    = Math.max(0, Pipe.takeByteArrayLength(this.pipe));
		assert(this.length<=this.pipe.sizeOfBlobRing) : "bad length "+this.length;
		
		this.bytesLowBound = this.position = Pipe.bytePosition(meta, this.pipe, this.length);
		this.backing   = Pipe.byteBackingArray(meta, this.pipe); 
		assert(this.backing!=null) : 
			"The pipe "+(1==(meta>>31)?" constant array ": " blob ")+"must be defined before use.\n "+this.pipe;
			
		this.bytesHighBound = this.pipe.blobMask & (this.position + this.length);
		
		assert(Pipe.validatePipeBlobHasDataToRead(this.pipe, this.position, this.length));
		
		return this.length;
    }
    
    public int peekLowLevelAPIField(int offset) {
    	int meta = Pipe.peekInt(this.pipe, offset);     
    	
    	this.isStructured = (0!=(Pipe.STRUCTURED_POS_MASK&meta));
    	
		this.length    = Math.max(0, Pipe.peekInt(this.pipe, offset+1));
		this.bytesLowBound = this.position = Pipe.convertToPosition(meta, this.pipe);
		this.backing   = Pipe.byteBackingArray(meta, this.pipe); 
		assert(this.backing!=null) : 
			"The pipe "+(1==(meta>>31)?" constant array ": " blob ")+"must be defined before use.\n "+this.pipe;
			
		this.bytesHighBound = this.pipe.blobMask & (this.position + this.length);
		
		assert(Pipe.validatePipeBlobHasDataToRead(this.pipe, this.position, this.length));
		
		return this.length;
    }
    
    
    @Deprecated
    public static <S extends MessageSchema<S>> int openLowLevelAPIField(DataInputBlobReader<S> that) {
        return that.openLowLevelAPIField();
    }

    public int accumLowLevelAPIField() {
    	return accumLowLevelAPIField(this);
    }
    
    public static int accumLowLevelAPIField(DataInputBlobReader<?> that) {
        if (0==that.length) {
            int meta = Pipe.takeByteArrayMetaData(that.pipe);
			int localLen = Pipe.takeByteArrayLength(that.pipe);
			
			that.length    = Math.max(0, localLen);
			that.bytesLowBound = that.position = Pipe.bytePosition(meta, that.pipe, that.length);
			that.backing   = Pipe.byteBackingArray(meta, that.pipe); 
			assert(that.backing!=null) : 
				"The pipe "+(1==(meta>>31)?" constant array ": " blob ")+"must be defined before use.\n "+that.pipe;
				
			that.bytesHighBound = that.pipe.blobMask & (that.position + that.length);
			
			assert(Pipe.validatePipeBlobHasDataToRead(that.pipe, that.position, that.length));
			
			return localLen;
        } else {        
        
            Pipe.takeByteArrayMetaData(that.pipe);
            int len = Pipe.takeByteArrayLength(that.pipe);
            if (len>0) {//may be -1 for null values
            	that.length += len;
            	that.bytesHighBound = that.pipe.blobMask & (that.bytesHighBound + len);
            }
            return len;
        }
    }
    
    
    public int accumHighLevelAPIField(int loc) {
        if (0>=this.length) {
        	
            int localLen = PipeReader.readBytesLength(pipe, loc);
			this.length         = Math.max(0, localLen);
			this.bytesLowBound  = this.position       = PipeReader.readBytesPosition(pipe, loc);
			this.backing        = PipeReader.readBytesBackingArray(pipe, loc); 
			assert(this.backing!=null) : "The pipe must be init before use.";
			this.bytesHighBound = pipe.blobMask & (position + length);
			
			assert(Pipe.validatePipeBlobHasDataToRead(pipe, position, length));
			
			return localLen;
        } else {        
        
        	int len = PipeReader.readBytesLength(pipe, loc);
        	if (len>0) {            
        		this.length += len;
            	this.bytesHighBound = pipe.blobMask & (bytesHighBound + len);
        	}
            return len;
        }        
    }
    
    @Override    
    public boolean hasRemainingBytes() {
        return (byteMask & position) != bytesHighBound;
    }

    @Override
    public int available() {        
        return bytesRemaining(this);
    }

    public static int bytesRemaining(DataInputBlobReader<?> that) {
    	return that.length - that.position();
    }

    public DataInput nullable() {
        return length<0 ? null : this;
    }
   
    @Override
    public int absolutePosition() {
    	return absolutePosition(this);
    }
    
    @Override
    public void absolutePosition(int position) {
    	absolutePosition(this, position);
    }
    
    public static int absolutePosition(DataInputBlobReader<?> reader) {
    	return reader.position;
    }
    
    public static void absolutePosition(DataInputBlobReader<?> reader, int position) {
    	reader.position = position;
    }
    
    public static void position(DataInputBlobReader<?> reader, int byteIndexFromStart) {
    	assert(byteIndexFromStart>=0);
    	//can read up to the end so this position is the same as length yet it is excluded.
    	assert(byteIndexFromStart<=reader.length) : "index of "+byteIndexFromStart+" is out of limit "+reader.length;
    	//logger.trace("set to position from start "+byteIndexFromStart);
    	reader.position = reader.bytesLowBound+byteIndexFromStart;
    }

    @Override
    public int position() {
    	return position - bytesLowBound;
    }    
    
	@Override
	public void position(int position) {
		position(this, position);
	}
    
    @Deprecated
    public void setPositionBytesFromStart(int byteIndexFromStart) {
        position(this, byteIndexFromStart);
    }

    
    
    
//    @Override
//	public synchronized void mark(int readlimit) {
//		super.mark(readlimit);
//	}
//
//	@Override
//	public synchronized void reset() throws IOException {
//		super.reset();
//	}
//
//	@Override
//	public boolean markSupported() {
//		return super.markSupported();
//	}

	@Override
    public int read(byte[] b) {
        if ((byteMask & position) == bytesHighBound) {
            return EOF_MARKER;
        }       
                
        int max = bytesRemaining(this);
        int len = b.length > max? max : b.length;      

        Pipe.copyBytesFromToRing(backing, position, byteMask, b, 0, Integer.MAX_VALUE, len);
        position += len;
        return len;
    }
    
    @Override
    public int read(byte[] b, int off, int len) {
        if ((byteMask & position) == bytesHighBound) {
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
    public long skip(long n) {
    	long count = Math.min(n, bytesRemaining(this));
    	assert(count+position < ((long)Integer.MAX_VALUE));
        position += count; 
        return count;
    }
    
    @Override
    public int skipBytes(int n) {
    	
    	int count = Math.min(n, bytesRemaining(this));
        position += count;
        return count;
    }

    @Override
    public boolean readBoolean() {
        return backing[byteMask & position++]>0; //positive is true, zero is false, neg is null
    }
    
    @Override
    public boolean wasBooleanNull() {
    	return backing[byteMask & (position-1)]<0;
    }

    @Override
    public byte readByte() {
        return backing[byteMask & position++];
    }

    @Override
    public int readUnsignedByte() {
        return 0xFF & backing[byteMask & position++];
    }
    
    private static <S extends MessageSchema<S>> short read16(byte[] buf, int mask, DataInputBlobReader<S> that) {
        return (short)((       buf[mask & that.position++] << 8) |
                       (0xFF & buf[mask & that.position++])); 
    }    
    
    private static <S extends MessageSchema<S>> int read32(byte[] buf, int mask, DataInputBlobReader<S> that) {        
        return ( ( (       buf[mask & that.position++]) << 24) |
                 ( (0xFF & buf[mask & that.position++]) << 16) |
                 ( (0xFF & buf[mask & that.position++]) << 8) |
                   (0xFF & buf[mask & that.position++]) ); 
    }
    
    private static <S extends MessageSchema<S>> long read64(byte[] buf, int mask, DataInputBlobReader<S> that) {        
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
        return (byteMask & position) != bytesHighBound ? backing[byteMask & position++] : EOF_MARKER;//isOpen?0:-1;
    }

    @Override
    public String readLine() {
        
        workspace.setLength(0);        
        if ((byteMask & position) != bytesHighBound) {
            char c = (char)read16(backing,byteMask,this);
            while (
                    ((byteMask & position) != bytesHighBound) &&  //hard stop for EOF but this is really end of field.
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
    public String readUTF() { //use this two line implementation
        int length = readShort(); //read first 2 byte for length in bytes to convert.
        return length>=0?readUTFOfLength(length):null;
    }
    
    @Override
    public <A extends Appendable> A readUTF(A target) {
        int length = readShort(); //read first 2 byte for length in bytes to convert.    
        return length>=0?readUTFOfLength(length, target):target;
    }

    @Override
    public String readUTFFully() {
        workspace.setLength(0);
        try {
        	return readUTF(this, available(), workspace).toString();
        } catch (Exception e) {
        	throw new RuntimeException(e);
        }
    }
    
    @Override
    public String readUTFOfLength(int length) {
    	if (length >= 0) {
	        workspace.setLength(0);
	        
	        try {
	        	return readUTF(this, length, workspace).toString();
	        } catch (Exception e) {
	        	throw new RuntimeException(e);
	        }
    	} else {
    		return null;
    	}
    }
    
    @Override
    public <A extends Appendable> A readUTFOfLength(int length, A target) {      
        try {
        	return readUTF(this, length, target);
        } catch (Exception e) {
        	throw new RuntimeException(e);
        }
    }
    
    @Override
    public long parse(TrieParserReader reader, TrieParser trie, int length) {
		long result = reader.query(reader, trie, backing, position, length, byteMask);
		
		if (result!=-1) {			
			position = reader.sourcePos;
		}
		
		return result;
	}
    
    @Override
	public boolean equalUTF(byte[] equalText) {
		int len = readShort();
		if (len!=equalText.length) {
			return false;
		}
		int p = 0;
		int pp = position;
		while (--len>=0) {
			if (equalText[p++]!=backing[byteMask & pp++]) {
				return false;
			}
		}
		//only moves forward when equal.
		position = pp;
		return true;
	}
	
    @Override
	public boolean equalBytes(byte[] bytes) {
		return equalBytes(bytes, 0, bytes.length);
	}
	
    @Override
	public boolean equalBytes(byte[] bytes, int bytesPos, int bytesLen) {
		int len = available();
		if (len<bytesLen) {
			//if its too short then it can not be equal, this is a stream so longer is ok.
			return false;
		}
		int pp = position;
		while (--bytesLen>=0) {
			if (bytes[bytesPos++]!=backing[byteMask & pp++]) {
				return false;
			}
		}
		//only moves forward when equal.
		position = pp;
		return true;
	}
	
	
    public static <A extends Appendable, S extends MessageSchema<S>> A readUTF(DataInputBlobReader<S> reader, int length, A target) throws IOException {
    	if (length>=0) {
	    	assert(reader.storeMostRecentPacked(-1));
	    	long charAndPos = ((long)reader.position)<<32;
	        long limit = ((long)reader.position+length)<<32;
	
	        while (charAndPos<limit) {
	            charAndPos = Pipe.decodeUTF8Fast(reader.backing, charAndPos, reader.byteMask);
	            target.append((char)charAndPos);
	        }
	        reader.position+=length;
    	}
        return target;
    }
        
    @Override
    public void readInto(Externalizable target) {
    	
    	try {
			target.readExternal(this);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    	
    }
    
    
    @Override
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
    public static <S extends MessageSchema<S>> int read(DataInputBlobReader<S> reader, byte[] b, int off, int len, int mask) {

        int max = bytesRemaining(reader);
        if (len > max) {
            len = max;
        }
        Pipe.copyBytesFromToRing(reader.backing, reader.position, reader.byteMask, b, off, mask, len);
        reader.position += len;
        return len;
    }
       
    @Override
    public void readInto(ChannelWriter writer, int length) {
    	
    	DataOutputBlobWriter.write((DataOutputBlobWriter<?>)writer, backing, position, length, byteMask);
    	position += length;
    	
    }    
    
    public <T extends MessageSchema<T>> void readInto(DataOutputBlobWriter<T> writer, int length) {
    	
    	DataOutputBlobWriter.write(writer, backing, position, length, byteMask);
    	position += length;
    	
    }   
    
    
    ////////
    //parsing methods
    ///////
    public static TrieParser textToNumberTrieParser() {
    	if (textToNumberParser == null) {    		
    		TrieParser p = new TrieParser(8,false); //supports streaming, so false
    		p.setUTF8Value("%i%.", 1); 
    		
    		
    		
    		//p.setUTF8Value("%i%.%/%.", 1);
    		//the above pattern can parse  234 and 234.34 and 23/45 and 23.4/56.7
    		//it also supports the normal capture methods for ints and decimals with no change
    		
    		//TrieParserReader.capturedLongField(parserReader, 0)
    		
    		
    		textToNumberParser = p;
    	}
    	return textToNumberParser;
    }
    
    private TrieParserReader parserReader() {
    	if (null == reader) {
    		reader = new TrieParserReader(true);
    	}
    	return reader;
    }
    
    public static long readUTFAsLong(DataInputBlobReader<?> reader) {    	
    	return convertTextToLong(reader.parserReader(), reader.backing, reader.position, reader.byteMask, reader.available());
    }

	public static long convertTextToLong(TrieParserReader parserReader,
			                             byte[] backing, 
			                             int position, 
			                             int mask,
			                             int available) {
		
		return (TrieParserReader.query(parserReader, 
				                   textToNumberTrieParser(), 
    			                   backing, position, 
    			                   available, mask)>=0)
				? TrieParserReader.capturedLongField(parserReader, 0) : -1;
	}
    
    public static double readUTFAsDecimal(DataInputBlobReader<?> reader) {
    	return convertTextToDouble(reader.parserReader(), reader.backing, reader.position, reader.available(), reader.byteMask);	
    }

	public static double convertTextToDouble(TrieParserReader parserReader, 
											 byte[] backing, int position,
											 int available, int mask) {
		
		return (TrieParserReader.query(parserReader, textToNumberTrieParser(), 
		    			               backing, position,
		    			               available, mask)>=0) ?
		    			            		   
    		Decimal.asDouble(
    				TrieParserReader.capturedDecimalMField(parserReader, 0), 
    				TrieParserReader.capturedDecimalEField(parserReader, 0)) : -1;
    				
	}
	
	public static boolean convertTextToDecimal(TrieParserReader parserReader, 
			 byte[] backing, int position,
			 int available, int mask, DecimalResult result) {

		if (TrieParserReader.query(parserReader, textToNumberTrieParser(), 
		      backing, position,
		      available, mask)>=0) {
			
			result.result(TrieParserReader.capturedDecimalMField(parserReader, 0), 
				    	  TrieParserReader.capturedDecimalEField(parserReader, 0));
			
			return true;
		} else {
			return false;
		}
		
	}
    
    ///////
    //Packed Chars
    //////
    @Override
    public <A extends Appendable> A readPackedChars(A target) {
        readPackedChars(this,target);
        return target;
    }
    
    public static <S extends MessageSchema<S>> void readPackedChars(DataInputBlobReader<S> that, Appendable target) {
        int length = readPackedInt(that);
        int i = length;
        try {
	        while (--i>=0) {
	            target.append((char) readPackedInt(that));
	        }
        } catch (IOException e) {
        	throw new RuntimeException(e);
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
    @Override
    public long readPackedLong() {   
            long result = readPackedLong(this);
            assert(storeMostRecentPacked(result));
            return result;
    }

    boolean storeMostRecentPacked(long result) {
    	mostRecentPacked=result;
    	return true;
	}

	@Override
    public final boolean wasPackedNull() {
    	assert checkRecentPacked(this) : "Must not check for null unless value was read as zero first";
        return wasPackedNull(this);
    }
    
    @Override
    public final boolean wasDecimalNull() {
    	assert checkRecentPacked(this) : "Must not check for null unless value was read as zero first";
    	return wasDecimalNull(this);
    }
    
    @Override
    public int readPackedInt() {   
        int result = readPackedInt(this);
        assert(storeMostRecentPacked(result));
        return result;
    }
    
    @Override
    public double readDecimalAsDouble() {
    	long m = readPackedLong();
    	assert(storeMostRecentPacked(m));
    	if (0!=m) {
    		return Decimal.asDouble(m, readByte());
    	} else {
    		position++;//must consume last byte (not needed);
    		return wasPackedNull() ? Double.NaN: 0;
    	}
    }
    
    @Override
	public <A extends Appendable> A readDecimalAsText(A target) {
		long m = readPackedLong();
		assert(storeMostRecentPacked(m));
		
		return Appendables.appendDecimalValue(target, m, readByte());
	}


    
    @Override
    public double readRationalAsDouble() {
    	return (double)readPackedLong()/(double)readPackedLong();
    }
    
    @Override
    public <A extends Appendable> A readRationalAsText(A target) {
    
    	try {
			return (A) Appendables.appendValue(
					Appendables.appendValue(target, readPackedLong())
			           .append("/"), readPackedLong());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}      
    	
    }
    
    @Override
    public long readDecimalAsLong() {
    	//packed nulls will appear as zero in this case.
    	long m = readPackedLong();
    	assert(storeMostRecentPacked(m));
		return Decimal.asLong(m, readByte());
    }
    
    @Override
    public short readPackedShort() {
        short result = (short)readPackedInt(this);
        assert(storeMostRecentPacked(result));
        return result;
    }

    public static <S extends MessageSchema<S>> Pipe<S> getBackingPipe(DataInputBlobReader<S> that) {
    	return that.pipe;
    }
    
    public static <S extends MessageSchema<S>> long readPackedLong(DataInputBlobReader<S> that) {
        byte v = that.backing[that.byteMask & that.position++];
        long accumulator = (~((long)(((v>>6)&1)-1)))&0xFFFFFFFFFFFFFF80l;
        return (v >= 0) ? readPackedLong((accumulator | v) << 7,that.backing,that.byteMask,that, 0) : (accumulator) |(v & 0x7F);
    }
    
    public static <S extends MessageSchema<S>> int readPackedInt(DataInputBlobReader<S> that) {
        byte v = that.backing[that.byteMask & that.position++];
        int accumulator = (~((int)(((v>>6)&1)-1)))&0xFFFFFF80; 
        return (v >= 0) ? readPackedInt((accumulator | v) << 7,that.backing,that.byteMask,that, 0) : accumulator |(v & 0x7F);
    }
    
    //recursive use of the stack turns out to be a good way to unroll this loop.
    private static <S extends MessageSchema<S>> long readPackedLong(long a, byte[] buf, int mask, DataInputBlobReader<S> that, int depth) {
        return readPackedLongB(a, buf, mask, that, buf[mask & that.position++], depth+1);
    }

    private static <S extends MessageSchema<S>> long readPackedLongB(long a, byte[] buf, int mask, DataInputBlobReader<S> that, byte v, int depth) {
    	assert(depth<11) : "Error malformed data";
    	if (depth<11) {
    		//Not checking for this assert because we use NaN for business logic, assert(a!=0 || v!=0) : "malformed data";
    		return (v >= 0) ? readPackedLong((a | v) << 7, buf, mask, that, depth) : a | (v & 0x7Fl);
    	} else {
    		logger.warn("malformed data");
    		return 0;
    	}
    }

    public static <S extends MessageSchema<S>> boolean wasPackedNull(DataInputBlobReader<S> that) {
     	return wasPackedNull(that.backing, that.byteMask, that.position);
    }
    
    public static <S extends MessageSchema<S>> boolean wasDecimalNull(DataInputBlobReader<S> that) {
       	return wasPackedNull(that.backing, that.byteMask, that.position-1); //skip over trailing e
    }

	private static <S extends MessageSchema<S>> boolean checkRecentPacked(DataInputBlobReader<S> that) {
		boolean result = 0 == that.mostRecentPacked;
		that.mostRecentPacked = -1;//can only be checked once.
		return result;
	}

	private static boolean wasPackedNull(byte[] localBacking, int localMask, int localPos) {
		assert( 0 != (0x80&localBacking[localMask & (localPos-1)]) ) : "Must only call wasPackedNull AFTER a packed number read";
    	
    	return (((byte)0x80)==localBacking[localMask & (localPos-1)])
    		   && 0==localBacking[localMask & (localPos-2)]
    	       && 0==localBacking[localMask & (localPos-3)];
	}
       
    private static <S extends MessageSchema<S>> int readPackedInt(int a, byte[] buf, int mask, DataInputBlobReader<S> that, int depth) {
    	return readPackedIntB(a, buf, mask, that, buf[mask & that.position++], depth+1);
    }

    private static <S extends MessageSchema<S>> int readPackedIntB(int a, byte[] buf, int mask, DataInputBlobReader<S> that, byte v, int depth) {
    	assert(depth<7) : "Error malformed data";
        return (v >= 0) ? readPackedInt((a | v) << 7, buf, mask, that, depth) : a | (v & 0x7F);
    }

    
	public static void setupParser(DataInputBlobReader<?> input, TrieParserReader reader) {
		
		//System.out.println("input data to be parsed: ");
		//Appendables.appendUTF8(System.out, input.backing, input.position, bytesRemaining(input), input.byteMask);
		
		TrieParserReader.parseSetup(reader, input.backing, input.position, bytesRemaining(input), input.byteMask); 
	}

	public static void setupParser(DataInputBlobReader<?> input, TrieParserReader reader, int length) {
		TrieParserReader.parseSetup(reader, input.backing, input.position, Math.min(bytesRemaining(input), length), input.byteMask); 
	}
	@Override
	public StructuredReader structured() {
		assert(isStructured()) : "This data is not structured";
		return structuredReader;
	}

	public static short peekShort(DataInputBlobReader<?> channelReader) {
        return (short)((       channelReader.backing[channelReader.byteMask & channelReader.position] << 8) |
        		       (0xFF & channelReader.backing[channelReader.byteMask & channelReader.position])); 
	}
	
	public int length() {
		return length;
	}

    
}
