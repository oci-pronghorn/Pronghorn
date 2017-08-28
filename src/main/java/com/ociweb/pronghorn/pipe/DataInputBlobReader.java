package com.ociweb.pronghorn.pipe;

import java.io.DataInput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.math.Decimal;

public class DataInputBlobReader<S extends MessageSchema<S>> extends BlobReader {

    private final StringBuilder workspace;
    private final Pipe<S> pipe;
    private byte[] backing;
    private final int byteMask;
    private static final Logger logger = LoggerFactory.getLogger(DataInputBlobReader.class);
    
    protected int length;
    private int bytesHighBound;
    protected int bytesLowBound;
    protected int position;

    private static TrieParser textToNumberParser;
    private TrieParserReader reader;
    
    private int EOF_MARKER = -1;
    
    public DataInputBlobReader(Pipe<S> pipe) {
    	super();
        this.pipe = pipe;
        this.backing = Pipe.blob(pipe);
        this.byteMask = Pipe.blobMask(pipe); 
        this.workspace = new StringBuilder(64);
        assert(this.backing!=null) : "The pipe must be init before use.";
    }
        
	public void debug() {
	    
    	Appendables.appendArray(Appendables.appendValue(System.out,  "read at ", bytesLowBound), '[', backing, bytesLowBound, byteMask, ']',  length);

	}
	
    public int openHighLevelAPIField(int loc) {
        
        this.length         = Math.max(0, PipeReader.readBytesLength(pipe, loc));
        this.bytesLowBound  = this.position       = PipeReader.readBytesPosition(pipe, loc);
        this.backing        = PipeReader.readBytesBackingArray(pipe, loc); 
        assert(this.backing!=null) : "The pipe must be init before use.";
        this.bytesHighBound = pipe.blobMask & (position + length);
        
        assert(Pipe.validatePipeBlobHasDataToRead(pipe, position, length));
        
        return this.length;
    }
    
    public static int peekHighLevelAPIField(DataInputBlobReader<?> reader, int loc) {
        
    	reader.length         = PipeReader.peekDataLength(reader.pipe, loc);
    	reader.bytesLowBound  = reader.position = PipeReader.peekDataPosition(reader.pipe, loc);
    	reader.backing        = PipeReader.peekDataBackingArray(reader.pipe, loc); 
    	assert(reader.backing!=null) : "The pipe must be init before use.";
    	reader.bytesHighBound = reader.pipe.blobMask & (reader.position + reader.length);
     
        return reader.length;
    }
    
    
    public int readFromEndLastInt(int negativeIntOffset) {
    	assert(negativeIntOffset>0) : "there is no data found at the end";
    	
    	//NOTE: we ignore length of the data and always read from maxVarLen 
    	
    	int position = (bytesLowBound + pipe.maxVarLen)-(4*negativeIntOffset);
    	
    	//logger.trace("read from last {}  {}  {}",bytesLowBound,pipe.maxVarLen,negativeIntOffset);
    	
//    	logger.info("reading int from position {} value {} from pipe {}",position,
//    			(backing[byteMask & position]<<24) |
//    			(backing[byteMask & (position+1)]<<16) |
//    			(backing[byteMask & (position+2)]<<8) |
//    			(backing[byteMask & (position+3)]),
//    			getBackingPipe(this).id
//    			);
    	   	
    	
    	return ( ( (       backing[byteMask & position++]) << 24) |
		 ( (0xFF & backing[byteMask & position++]) << 16) |
		 ( (0xFF & backing[byteMask & position++]) << 8) |
		   (0xFF & backing[byteMask & position++]) );
    }
    
    public int openLowLevelAPIField() {
        int meta = Pipe.takeRingByteMetaData(this.pipe);
		this.length    = Math.max(0, Pipe.takeRingByteLen(this.pipe));
		this.bytesLowBound = this.position = Pipe.bytePosition(meta, this.pipe, this.length);
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
        if (0==this.length) {
            return openLowLevelAPIField();
        } else {        
        
            Pipe.takeRingByteMetaData(pipe);
            int len = Pipe.takeRingByteLen(pipe);
            if (len>0) {//may be -1 for null values
            	this.length += len;
            	this.bytesHighBound = pipe.blobMask & (bytesHighBound + len);
            }
            return len;
        }
    }
    
    
    public int accumHighLevelAPIField(int loc) {
        if (0>=this.length) {
            return openHighLevelAPIField(loc);
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
        return bytesRemaining(that, that.byteMask & that.position);
    }

	private static int bytesRemaining(DataInputBlobReader<?> that, int maskPos) {
		return  that.bytesHighBound >= maskPos ? 
        		that.bytesHighBound - maskPos : 
        		(that.pipe.sizeOfBlobRing - maskPos) + that.bytesHighBound;
	}

    public DataInput nullable() {
        return length<0 ? null : this;
    }
   
    public int absolutePosition() {
    	return absolutePosition(this);
    }
    
    public void absolutePosition(int position) {
    	absolutePosition(this, position);
    }
    
    public static int absolutePosition(DataInputBlobReader<?> reader) {
    	return reader.position;
    }
    
    public static void absolutePosition(DataInputBlobReader<?> reader, int position) {
    	reader.position = position;
    }
    
    public void setPositionBytesFromStart(int byteIndexFromStart) {
    	assert(byteIndexFromStart<length) : "index of "+byteIndexFromStart+" is out of limit "+length;
    	//logger.trace("set to position from start "+byteIndexFromStart);
    	position = bytesLowBound+byteIndexFromStart;
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
        return readUTFOfLength(length);
    }
    
    @Override
    public <A extends Appendable> A readUTF(A target) {
        int length = readShort(); //read first 2 byte for length in bytes to convert.    
        return readUTFOfLength(length, target);
    }
    
    @Override
    public String readUTFOfLength(int length) {
        workspace.setLength(0);
        try {
        	return readUTF(this, length, workspace).toString();
        } catch (Exception e) {
        	throw new RuntimeException(e);
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
		if (len!=bytesLen) {
			return false;
		}
		int pp = position;
		while (--len>=0) {
			if (bytes[bytesPos++]!=backing[byteMask & pp++]) {
				return false;
			}
		}
		//only moves forward when equal.
		position = pp;
		return true;
	}
	
	
    public static <A extends Appendable, S extends MessageSchema<S>> A readUTF(DataInputBlobReader<S> reader, int length, A target) throws IOException {
        long charAndPos = ((long)reader.position)<<32;
        long limit = ((long)reader.position+length)<<32;
        assert(length <= reader.available()) : "malformed data";

        while (charAndPos<limit) {
            charAndPos = Pipe.decodeUTF8Fast(reader.backing, charAndPos, reader.byteMask);
            target.append((char)charAndPos);
        }
        reader.position+=length;
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
    public void readInto(BlobWriter writer, int length) {
    	
    	DataOutputBlobWriter.write((DataOutputBlobWriter)writer, backing, position, length, byteMask);
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
    		TrieParser p = new TrieParser(32,false);
    		p.setUTF8Value("%i%.", 1);
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
    	TrieParserReader parserReader = reader.parserReader();
    	long token = TrieParserReader.query(parserReader, textToNumberTrieParser(), 
    			               reader.backing, reader.position,
    			               reader.available(), reader.byteMask);
    	
    	if (token>=0) {
    		return TrieParserReader.capturedLongField(reader.reader, 0);
    	} else {
    		return -1;//none
    	}    	
    }
    
    public static double readUTFAsDecimal(DataInputBlobReader<?> reader) {
    	TrieParserReader parserReader = reader.parserReader();
    	long token = TrieParserReader.query(parserReader, textToNumberTrieParser(), 
    			               reader.backing, reader.position,
    			               reader.available(), reader.byteMask);
    	
    	if (token>=0) {
    		long m = TrieParserReader.capturedDecimalMField(reader.reader, 0);
    		byte e = TrieParserReader.capturedDecimalEField(reader.reader, 0);
    		return Decimal.asDouble(m, e);
    		
    	} else {
    		return -1;//none
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
            return readPackedLong(this);
    }

    @Override
    public int readPackedInt() {   
        return readPackedInt(this);
    }
    
    @Override
    public double readDecimalAsDouble() {
    	return Decimal.asDouble(readPackedLong(), readByte());
    }
    
    @Override
    public double readRationalAsDouble() {
    	return (double)readPackedLong()/(double)readPackedLong();
    }
    
    @Override
    public long readDecimalAsLong() {
    	return Decimal.asLong(readPackedLong(), readByte());
    }
    
    @Override
    public short readPackedShort() {
        return (short)readPackedInt(this);
    }

    public static <S extends MessageSchema<S>> Pipe<S> getBackingPipe(DataInputBlobReader<S> that) {
    	return that.pipe;
    }
    
    public static <S extends MessageSchema<S>> long readPackedLong(DataInputBlobReader<S> that) {
        byte v = that.backing[that.byteMask & that.position++];
        long accumulator = (~((long)(((v>>6)&1)-1)))&0xFFFFFFFFFFFFFF80l;
        return (v >= 0) ? readPackedLong((accumulator | v) << 7,that.backing,that.byteMask,that) : (accumulator) |(v & 0x7F);
    }

    public static <S extends MessageSchema<S>> int readPackedInt(DataInputBlobReader<S> that) {
        byte v = that.backing[that.byteMask & that.position++];
        int accumulator = (~((int)(((v>>6)&1)-1)))&0xFFFFFF80; 
        return (v >= 0) ? readPackedInt((accumulator | v) << 7,that.backing,that.byteMask,that) : accumulator |(v & 0x7F);
    }
    
    //recursive use of the stack turns out to be a good way to unroll this loop.
    private static <S extends MessageSchema<S>> long readPackedLong(long a, byte[] buf, int mask, DataInputBlobReader<S> that) {
        return readPackedLongB(a, buf, mask, that, buf[mask & that.position++]);
    }

    private static <S extends MessageSchema<S>> long readPackedLongB(long a, byte[] buf, int mask, DataInputBlobReader<S> that, byte v) {
        assert(a!=0 || v!=0) : "malformed data";
        return (v >= 0) ? readPackedLong((a | v) << 7, buf, mask, that) : a | (v & 0x7Fl);
    }
       
    private static <S extends MessageSchema<S>> int readPackedInt(int a, byte[] buf, int mask, DataInputBlobReader<S> that) {
        return readPackedIntB(a, buf, mask, that, buf[mask & that.position++]);
    }

    private static <S extends MessageSchema<S>> int readPackedIntB(int a, byte[] buf, int mask, DataInputBlobReader<S> that, byte v) {
        assert(a!=0 || v!=0) : "malformed data";
        return (v >= 0) ? readPackedInt((a | v) << 7, buf, mask, that) : a | (v & 0x7F);
    }

	public static void setupParser(DataInputBlobReader<?> input, TrieParserReader reader) {
		TrieParserReader.parseSetup(reader, input.backing, input.position, bytesRemaining(input), input.byteMask); 
	}

	public static void setupParser(DataInputBlobReader<?> input, TrieParserReader reader, int length) {
		TrieParserReader.parseSetup(reader, input.backing, input.position, Math.min(bytesRemaining(input), length), input.byteMask); 
	}
    
    
    
}
