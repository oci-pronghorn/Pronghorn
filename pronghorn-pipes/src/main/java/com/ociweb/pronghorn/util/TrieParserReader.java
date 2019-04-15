package com.ociweb.pronghorn.util;

import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.struct.ByteSequenceValidator;
import com.ociweb.pronghorn.struct.DecimalValidator;
import com.ociweb.pronghorn.struct.LongValidator;
import com.ociweb.pronghorn.util.math.Decimal;

public class TrieParserReader {

	private static final int LONGEST_LONG_HEX_DIGITS = 16;

	private static final int LONGEST_LONG_DIGITS = 19;

	private static final Logger logger = LoggerFactory.getLogger(TrieParserReader.class);

	private byte[] sourceBacking;
	public int     sourcePos;
	public int     sourceLen;
	public int     sourceMask;

	private static final int NUMERIC_TYPE_MASK            =  0x03F; //shift zero this is low
	private static final int NUMERIC_LENGTH_MASK          = 0x01FF; //shift 6 (512)
	private static final int NUMERIC_LENGTH_SHIFT         =      6; //				
	private static final int NUMERIC_ABSENT_IS_ZERO_MASK  = 0x8000;      
	
	private int[]  capturedValues;
	private int    capturedValuesLength = 0;
	
	private int    capturedPos; //using sourceBacking



	private long    safeReturnValue = -1;
	private int     safeCapturedPos = -1;
	private int     saveCapturedLen = -1;
	private int     safeSourcePos = -1;

	private long result;
	
	private long unfoundConstant;
	private long noMatchConstant;
	
	
	private boolean normalExit;

	private final int MAX_TEXT_LENGTH = 1024;
	private transient Pipe<RawDataSchema> workingPipe = RawDataSchema.instance.newPipe(2,MAX_TEXT_LENGTH);

	private final static int MAX_ALT_DEPTH = 256; //full recursion on alternate paths from a single point.
	private int altStackPos = 0;
	private static final int fieldsOnStack = 4;
	private int[] altStack = new int[MAX_ALT_DEPTH*fieldsOnStack];

	private short[] workingMultiStops = new short[MAX_ALT_DEPTH];
	private int[]   workingMultiContinue = new int[MAX_ALT_DEPTH];



	private int pos;
	private int runLength;
	private int type;
	private int localSourcePos;

	public String toString() {
		return "Pos:"+sourcePos+" Len:"+sourceLen;
	}

	//TODO: when looking for N stops or them together as a quick way to avoid a number of checks.

	public void debug() {
		System.err.println(TrieParserReader.class.getName()+" reader debug() details:");
		System.err.println("pos  "+sourcePos+" masked "+(sourcePos&sourceMask));
		System.err.println("len  "+sourceLen);
		System.err.println("mask "+sourceMask);
		System.err.println("size "+sourceBacking.length);

	}

	private final boolean alwaysCompletePayloads;

	public TrieParserReader() {
		this(false);
	}

	public TrieParserReader(boolean alwaysCompletePayloads) {
		
		this.alwaysCompletePayloads = alwaysCompletePayloads;		
		workingPipe.initBuffers();
		
	}


	/**
	 * Given a visitor reads every path unless visitor returns false on open, in that case that branch will not be followed.
	 * When visit reaches the end the value of the byte array is returned.
	 * 
	 * Use Cases:
	 * 
	 *  We have a "template" and need to find all the existing paths that match it. For example find which known topics match a new subscription in MQTT.
	 *  
	 *  //NOTE: if we add the new type TYPE_VALUE_BYTES
	 *  We have a new path and need to find all the values in the tree that match it.  For example find which known subscriptions a new topic should go to in MQTT. 
	 * 
	 * TODO: Build test that rebuilds the full list of strings and their associated values.
	 *  
	 * 
	 */

	public void visit(TrieParser that, ByteSquenceVisitor visitor, byte[] source, int localSourcePos, int sourceLength, int sourceMask) {
		visit(that, 0, visitor, source, localSourcePos, sourceLength, sourceMask, -1, -1);
	}
	
	public void visit(TrieParser that, ByteSquenceVisitor visitor, byte[] source, int localSourcePos, int sourceLength, int sourceMask, long unfound, long noMatch) {
		visit(that, 0, visitor, source, localSourcePos, sourceLength, sourceMask, unfound, noMatch);
	}
	

	private void visit(TrieParser that, final int i, ByteSquenceVisitor visitor, byte[] source, int localSourcePos, int sourceLength, int sourceMask, final long unfoundResult, final long noMatchResult) {
		if (that.getLimit()==0) {
			return;//nothing to do, we have no patterns
		}
		
		int run = 0;
		short[] data = that.data;
		

		visitorInitForQuery(this, that, source, localSourcePos, unfoundResult, noMatchResult);
		this.pos = i+1; //used globally by other methods TODO: VERY BAD DESIGN THIS MAY CAUSE CORRUPT READS IN THE STACK
		if (this.pos>=data.length) {
			return;
		}
		assert i<that.data.length: "the jumpindex: " + i + " exceeds the data length: "+that.data.length;
		type = that.data[i]; //used globally by other methods TODO: VERY BAD DESIGN THIS MAY CAUSE CORRUPT READS IN THE STACK
		assert (type>-1) && (type<8) : "TYPE is not in range (0-7)";
		
		switch (type) {
		
			case TrieParser.TYPE_SWITCH_BRANCH:
		
				visitorSwitchBranch(that, i, visitor, source, localSourcePos, sourceLength, sourceMask, data);
				
				break;
		
			case TrieParser.TYPE_RUN:
	
				visitorRun(that, visitor, source, localSourcePos, sourceLength, sourceMask, data);
	
				break;
	
			case TrieParser.TYPE_BRANCH_VALUE:
	
				visitorBranch(that, visitor, source, localSourcePos, sourceLength, sourceMask, data);
	
				break;
	
			case TrieParser.TYPE_ALT_BRANCH:
	
				visitorAltBranch(that, i, visitor, source, localSourcePos, sourceLength, sourceMask, data);
	
				break;
	
			case TrieParser.TYPE_VALUE_NUMERIC:
	
				visitorNumeric(that, i, visitor, source, localSourcePos, sourceLength, sourceMask, run);
	
				break;
	
			case TrieParser.TYPE_VALUE_BYTES:
	
				visitorValueBytes(that, i, visitor, source, localSourcePos, sourceLength, sourceMask);
	
				break;
	
			case TrieParser.TYPE_SAFE_END:
	
				visitorSafeEnd(that, visitor, source, localSourcePos, sourceLength, sourceMask);
	
				break;
	
			case TrieParser.TYPE_END:
	
				visitorEnd(that, i, visitor);
	
				break;
			default:
				throw new UnsupportedOperationException("ERROR Unrecognized value\n");
		}
	}


	private void visitorEnd(TrieParser that, final int i, ByteSquenceVisitor visitor) {
		this.result = (0XFFFF&that.data[i+1]);
		//add to result set
		visitor.addToResult(this.result);
	}

	private void visitorSafeEnd(TrieParser that, ByteSquenceVisitor visitor, byte[] source, int localSourcePos,
			int sourceLength, int sourceMask) {
		recordSafePointEnd(this, localSourcePos, pos, that);  
		pos += that.SIZE_OF_RESULT;
		if (sourceLength == localSourcePos) {
			this.result = useSafePointNow(this);

			//add to result set
			visitor.addToResult(this.result);

			return;
		}   

		else{
			//recurse visit
			visit(that, this.pos, visitor, source, localSourcePos, sourceLength, sourceMask, this.unfoundConstant, this.noMatchConstant);
		}
	}

	private void visitorValueBytes(TrieParser that, final int i, ByteSquenceVisitor visitor, byte[] source,
			int localSourcePos, int sourceLength, int sourceMask) {
		int idx;
		int temp_pos;
		short stopValue;
		int byte_size = that.data[i+1];
		stopValue = that.data[pos++];
		idx = i + TrieParser.SIZE_OF_VALUE_BYTES;


		/*
		 * This will result the position, after parsing all the bytes if any
		 */
		if((temp_pos=parseBytes(this, source, localSourcePos, byte_size-localSourcePos, sourceMask, stopValue))<0){

			return;
		}
		localSourcePos = temp_pos;

		if(stopValue==byte_size){
			byte_size = 0;
		}

		//recurse into visit()
		visit(that, idx+byte_size, visitor, source, localSourcePos, sourceLength, sourceMask, this.unfoundConstant, this.noMatchConstant);
	}

	private void visitorNumeric(TrieParser that, final int i, ByteSquenceVisitor visitor, byte[] source,
			int localSourcePos, int sourceLength, int sourceMask, int run) {
		int idx;
		int temp_pos=0;
		idx = i + TrieParser.SIZE_OF_VALUE_NUMERIC;
		int templateLimit = Integer.MAX_VALUE;

		if (this.runLength<sourceLength &&
			(temp_pos = parseNumeric(that.ESCAPE_BYTE, this, source, localSourcePos, sourceLength, sourceMask, that.data[pos++]))<0){
			return;
		}
		localSourcePos = temp_pos;

		//recurse into visit()
		visit(that, idx+run, visitor, source, localSourcePos, sourceLength, sourceMask, this.unfoundConstant, this.noMatchConstant);
	}
	

	private void visitorSwitchBranch(TrieParser that, int i, ByteSquenceVisitor visitor, byte[] source,
					int localSourcePos, int sourceLength, int sourceMask, short[] data) {
	
		if (this.runLength<sourceLength) {          
		
			short metaData = data[pos]; 
			
			short trieLen  = (short)(metaData & 0xFF);//Also needed when we grow the switch on insert later
			short offset  = (short)((metaData>>8) & 0xFF);
			
			//we only have 8 sizes of jump tables made up of pairs of shorts.
			// 2 up to 512		
			int base = pos+1;//must keep since pos will be moving forward.			
			
			for(int k = 0; k<trieLen; k++) {
			
				//jump to new position, all are relative to the end of the jump table so no values need to be
				//adjusted if the jump table grows with new inserts.
				int j = k<<1;
				int idxJump = (((int)data[base+j])<<15) | (0x7FFF&data[base+j+1]);
				if (idxJump>=0) {
					visit(that, idxJump+(base-1+(trieLen<<1)), visitor, source, localSourcePos, sourceLength, sourceMask, this.unfoundConstant, this.noMatchConstant);//only that jump
				}
			
			}
		
		} else {
			return;
		}
	}
	

	private void visitorBranch(TrieParser that, ByteSquenceVisitor visitor, byte[] source, int localSourcePos,
			int sourceLength, int sourceMask, final short[] data) {

		if (this.runLength<sourceLength) {          
			//TrieMap data
			final int p = 1+pos;
			pos = (0==(0xFFFFFF&TrieParser.computeJumpMask((short) source[sourceMask & localSourcePos], data[pos]))) ? 3+pos :  2+p+((((int)data[p])<<15) | (0x7FFF&data[1+p]));
			visit(that, pos, visitor, source, localSourcePos, sourceLength, sourceMask, this.unfoundConstant, this.noMatchConstant);//only that jump
		} else{
			return;
		}
	}
	
	private void visitorAltBranch(TrieParser that, final int i,
			ByteSquenceVisitor visitor, byte[] source,
			int localSourcePos, int sourceLength, int sourceMask, 
			short[] data) {
		int localJump = i + TrieParser.SIZE_OF_ALT_BRANCH;
		int jump = (((int)data[pos++])<<15) | (0x7FFF&data[pos++]); 
		
		visit(that, localJump, visitor, source, localSourcePos, sourceLength, sourceMask, this.unfoundConstant, this.noMatchConstant);// near byte
		visit(that, localJump+jump, visitor, source, localSourcePos, sourceLength, sourceMask, this.unfoundConstant, this.noMatchConstant);// 2nd call with branch
	}

	private void visitorRun(TrieParser that, ByteSquenceVisitor visitor, byte[] source, int localSourcePos,
			int sourceLength, int sourceMask, short[] data) {

		byte caseMask = that.caseRuleMask;
		int r1 = data[pos]; 
		int t1 = pos +1;  
		int t2 = localSourcePos; 

		while ((--r1 >= 0) && ((caseMask&data[t1++]) == (caseMask&0xFF&source[sourceMask & t2++])) ) { //getting slash somewhere should not equal eachother. 
			//matching characters while decrementing run length.	
		}
		pos = t1;
		localSourcePos = t2;

		int r = r1;
		if (r >= 0) {
			return;	
		} else {        

			//int idx = pos + TrieParser.SIZE_OF_RUN-1;
			
			//visit(that, idx+run, visitor, source, localSourcePos+run, sourceLength, sourceMask, unfoundResult);
			//visit(that, pos, visitor, source, localSourcePos+run, sourceLength, sourceMask, unfoundResult);
			if (pos<that.data.length) {
				visit(that, pos, visitor, source, localSourcePos, sourceLength, sourceMask, this.unfoundConstant, this.noMatchConstant); //** took run off localsourcePos.
			}
			
		}
	}

	private static void visitorInitForQuery(TrieParserReader reader, TrieParser trie, byte[] source, int sourcePos, long unfoundResult, long noMatchResult) {
		reader.capturedPos = 0;
		reader.sourceBacking = source;
		//working vars
		reader.pos = 0;
		reader.runLength = 0;
		reader.localSourcePos = sourcePos;
		
		reader.result = unfoundResult;		
		reader.unfoundConstant = unfoundResult;
		reader.noMatchConstant = noMatchResult;
		
		reader.normalExit = true;
		reader.altStackPos = 0; 
		
		if (trie.maxExtractedFields()>0) {
			if (null==reader.capturedValues || (reader.capturedValues.length>>2)<trie.maxExtractedFields()) {
				reader.capturedValues = new int[4*(1+trie.maxExtractedFields())*4];
			}		
		}

		assert(trie.getLimit()>0) : "SequentialTrieParser must be setup up with data before use.";

		reader.type = trie.data[reader.pos++];
	}



	public static void parseSetup(TrieParserReader that, byte[] source, int offset, int length, int mask) {
		assert(length<=source.length) : "length is "+length+" but the array is only "+source.length;
		that.sourceBacking = source;	
		that.sourcePos     = offset;
		assert(that.sourcePos>=0) : "Negative source position offsets are not supported.";
		that.sourceLen     = length;
		that.sourceMask    = mask;
		
		assert(that.sourceLen <= ((long)that.sourceMask) + 1) : 
			  "ERROR the source length is larger than the backing array. "+that.sourceLen+" > "+(that.sourceMask + 1);

	}



	public static void parseSetupGrow(TrieParserReader that, int additionalLength) {
		that.sourceLen += additionalLength;
		assert(that.sourceLen <= that.sourceMask) : "length is out of bounds";
	}

	/**
	 * Save position and return the current length
	 * @param that
	 * @param target
	 * @param offset
	 * @return length of remaining position.
	 */
	public static int savePositionMemo(TrieParserReader that, int[] target, int offset) {

		target[offset] = that.sourcePos & that.sourceMask;
		return target[offset+1] = that.sourceLen;
		
	}


	public static void loadPositionMemo(TrieParserReader that, int[] source, int offset) {

		that.sourcePos = source[offset];
		that.sourceLen = source[offset+1];

	}


	public void moveBack(int i) {
		sourcePos -= i;
		sourceLen += i;
	}


	public static int debugAsUTF8(TrieParserReader that, Appendable target) {
		return debugAsUTF8(that,target, Integer.MAX_VALUE);
	}
	public static int debugAsUTF8(TrieParserReader that, Appendable target, int maxLen) {
		return debugAsUTF8(that, target, maxLen, true);
	}
	

	public static void debugAsArray(TrieParserReader reader, PrintStream err, int len) {
		
		Appendables.appendArray(System.err, reader.sourceBacking, reader.sourcePos, reader.sourceMask, Math.min(len, reader.sourceLen));

	}
	
	public static int debugAsUTF8(TrieParserReader that, Appendable target, int maxLen, boolean mayHaveLeading) {
		int pos = that.sourcePos;
		int slen = that.sourceLen;
		try {
			if (mayHaveLeading && ((that.sourceBacking[pos & that.sourceMask]<32) || (that.sourceBacking[(1+pos) & that.sourceMask]<32))) {
				//we have a leading length
				target.append("[");
				Appendables.appendValue(target, that.sourceBacking[that.sourceMask & pos++]);
				target.append(",");
				Appendables.appendValue(target, that.sourceBacking[that.sourceMask & pos++]);
				target.append("]");
				slen-=2;
			}

			int len = Math.min(maxLen, slen);
			if (len>0) {
				Appendable a = Appendables.appendUTF8(target, that.sourceBacking, pos, len, that.sourceMask);
				if (maxLen<slen) {
					a.append("...");
				}
			}
		} catch (Exception e) {
			//this was not UTF8 so dump the chars
			Appendables.appendArray(target, '[', that.sourceBacking, pos, that.sourceMask, ']', Math.min(maxLen, slen));
		}
		return pos;
	}

	public static boolean parseHasContent(TrieParserReader reader) {
		return reader.sourceLen>0;
	}

	public static int parseHasContentLength(TrieParserReader reader) {
		return reader.sourceLen;
	}

	public long parseNext(TrieParser trie) {
		return parseNext(this,trie);
	}

	public static long parseNext(TrieParserReader reader, TrieParser trie) {
		return parseNext(reader,trie,-1,-1);
	}
	
	public static long parseNext(TrieParserReader reader, TrieParser trie, final long unfound, final long notAnyPossibleMatch) {

		final int originalPos = reader.sourcePos;
		final int originalLen = reader.sourceLen;   

		long result =  query(reader, trie, reader.sourceBacking, originalPos, originalLen, reader.sourceMask, unfound, notAnyPossibleMatch);

		//Hack for now
		if (reader.sourceLen < 0) {
			//logger.info("warning trieReader is still walking past end");
			//TODO: URGENT FIX requred, this is an error in the trieReader the pattern "%b: %b\r\n" goes past the end and must be invalidated
			result = unfound;//invalidate any selection
		}
		//end of hack


		if (result!=unfound && result!=notAnyPossibleMatch) {
			return result;
		} else {
			//not found so roll the pos and len back for another try later
			reader.sourcePos = originalPos;
			reader.sourceLen = originalLen;
			return result;
		}

	}

	private static String debugContent(TrieParserReader reader, int debugPos, int debugLen) {
		return Appendables.appendUTF8(new StringBuilder(), 
				reader.sourceBacking, 
				debugPos, 
				Math.min(500,(int)debugLen), 
				reader.sourceMask).toString();
	}



	public int parseSkip(int count) {
		return parseSkip(this, count);
	}

	public static int parseSkip(TrieParserReader reader, int count) {

		int len = Math.min(count, reader.sourceLen);
		reader.sourcePos += len;
		reader.sourceLen -= len;

		return len;
	}

	public int parseSkipOne() {
		return parseSkipOne(this);
	}

	public static int parseSkipOne(TrieParserReader reader) {

		if (reader.sourceLen>=1) {
			int result = reader.sourceBacking[reader.sourcePos & reader.sourceMask];
			reader.sourcePos++;
			reader.sourceLen--;
			return 0xFF & result;
		} else {
			return -1;
		}

	}

	public static boolean parseSkipUntil(TrieParserReader reader, int target) {    	
		//skip over everything until we match the target, then we can parse from that point
		while ((reader.sourceLen > 0) && (reader.sourceBacking[reader.sourcePos & reader.sourceMask] != target )) {
			reader.sourcePos++;
		}

		return reader.sourceLen > 0;
	}


	public static int parseCopy(TrieParserReader reader, long count, DataOutputBlobWriter<?> writer) {

		int len = (int)Math.min(count, (long)reader.sourceLen);    	
		DataOutputBlobWriter.write(writer, reader.sourceBacking, reader.sourcePos, len, reader.sourceMask);    	
		reader.sourcePos += len;
		reader.sourceLen -= len;     
		assert(reader.sourceLen>=0);
		return len;
	}

	/**
	 * Gather until stop value is reached.
	 * @param reader
	 * @param stop
	 */
	public static void parseGather(TrieParserReader reader, DataOutput output, final byte stop) {

		byte[] source = reader.sourceBacking;
		int    mask   = reader.sourceMask;
		int    pos    = reader.sourcePos;
		// long    len    = reader.sourceLen;

		try {
			byte   value;        
			while(stop != (value=source[mask & pos++]) ) {
				output.writeByte(value);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			reader.sourcePos = pos;
		}

	}

	public static void parseGather(TrieParserReader reader, final byte stop) {

		byte[] source = reader.sourceBacking;
		int    mask   = reader.sourceMask;
		int    pos    = reader.sourcePos;
		//long    len    = reader.sourceLen;

		byte   value;        
		while(stop != (value=source[mask & pos++]) ) {
		}
		reader.sourcePos = pos;

	}    

	public static long query(TrieParserReader trieReader, TrieParser trie, Pipe<?> input, final long unfoundResult) {
		int meta = Pipe.takeByteArrayMetaData(input);
		int length    = Pipe.takeByteArrayLength(input);
		return query(trieReader, trie, Pipe.byteBackingArray(meta, input), Pipe.bytePosition(meta, input, length), length, Pipe.blobMask(input), unfoundResult );  
	}


	public static long query(TrieParserReader reader, TrieParser trie, 
			                 byte[] source, int localSourcePos, int sourceLength, int sourceMask) {
		return query(reader,trie,source,localSourcePos, sourceLength, sourceMask, -1);
	}


	public static long query(TrieParserReader reader, TrieParser trie, 
			                 byte[] source, int sourcePos, long sourceLength, int sourceMask, 
			                 final long unfoundResult) {

		return (TrieParser.getLimit(trie)>0) ? query2(reader, trie, source, sourcePos, sourceLength, sourceMask, unfoundResult, unfoundResult): unfoundResult;
		
	}
	
	public static long query(TrieParserReader reader, TrieParser trie, 
	            byte[] source, int sourcePos, long sourceLength, int sourceMask, 
	            final long unfoundResult, final long noMatchResult) {
	
		return (TrieParser.getLimit(trie)>0) ? query2(reader, trie, source, sourcePos, sourceLength, sourceMask, unfoundResult, noMatchResult): unfoundResult;
	}

	private static long query2(TrieParserReader reader, TrieParser trie, byte[] source, int sourcePos,
							  long sourceLength, int sourceMask, final long unfoundResult, final long noMatchResult) {
		
		initForQuery(reader, trie, source, sourcePos & Pipe.BYTES_WRAP_MASK, sourceMask, unfoundResult, noMatchResult);        
		processEachType(reader, trie, source, sourceLength, sourceMask, false, 0, -1);	
		return (reader.normalExit) ? exitUponParse(reader, trie) :   reader.result;
	}

	public long query(TrieParser trie, CharSequence cs) {
		return query(this, trie, cs);
	}
	
    public static long query(TrieParserReader reader, TrieParser trie, CharSequence cs) {
        
    	if ((cs.length()*6) > reader.workingPipe.maxVarLen) {
    		reader.workingPipe = RawDataSchema.instance.newPipe(2,cs.length()*6);
    		reader.workingPipe.initBuffers();
    	}
    	
        Pipe.addMsgIdx(reader.workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
        
        int origPos = Pipe.getWorkingBlobHeadPosition(reader.workingPipe);
        int len = Pipe.copyUTF8ToByte(cs, 0, cs.length(), reader.workingPipe);
        Pipe.addBytePosAndLen(reader.workingPipe, origPos, len);        
        Pipe.publishWrites(reader.workingPipe);
        Pipe.confirmLowLevelWrite(reader.workingPipe, Pipe.sizeOf(reader.workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        ///
        
        Pipe.takeMsgIdx(reader.workingPipe);
        long result = TrieParserReader.query(reader,trie,reader.workingPipe,-1); 
        Pipe.confirmLowLevelRead(reader.workingPipe, Pipe.sizeOf(reader.workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        Pipe.releaseReadLock(reader.workingPipe);
        
        return result;
    }

    public static ChannelWriter blobQueryPrep(TrieParserReader reader) {
     	 Pipe.addMsgIdx(reader.workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
    	 DataOutputBlobWriter<RawDataSchema> writer = Pipe.outputStream(reader.workingPipe);
    	 DataOutputBlobWriter.openField(writer);
    	 return writer;
    }
    
    public static long blobQuery(TrieParserReader reader, TrieParser trie) {
    	
        Pipe.outputStream(reader.workingPipe).closeLowLevelField();
    	Pipe.publishWrites(reader.workingPipe);
        Pipe.confirmLowLevelWrite(reader.workingPipe, Pipe.sizeOf(reader.workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
         
    	///
        
        Pipe.takeMsgIdx(reader.workingPipe);
        long result = TrieParserReader.query(reader,trie,reader.workingPipe,-1); 
        Pipe.confirmLowLevelRead(reader.workingPipe, Pipe.sizeOf(reader.workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        Pipe.releaseReadLock(reader.workingPipe);
        
        return result;
    }
    

	private static long exitUponParse(TrieParserReader reader, TrieParser trie) {
		reader.sourceLen -= (reader.localSourcePos-reader.sourcePos);
		reader.sourcePos = reader.localSourcePos;        	        	
		return TrieParser.readEndValue(trie.data,reader.pos, trie.SIZE_OF_RESULT);
	}

	private static void processEachType(TrieParserReader reader, 
			TrieParser trie, byte[] source, long sourceLength,
			int sourceMask, 
			boolean hasSafePoint,
			int t, int lastType) {

		reader.pos = 0;
		reader.type = trie.data[reader.pos++];	
		
		while (reader.normalExit && (t=reader.type) != TrieParser.TYPE_END ) {  
			
			if (TrieParser.TYPE_RUN == t) {   
				parseRun(reader, trie, source, sourceLength, sourceMask, hasSafePoint);
			} else {	
				if (TrieParser.TYPE_ALT_BRANCH == t) {
					processAltBranch(reader, source, trie.data, hasSafePoint);
				} else {
					if (TrieParser.TYPE_SWITCH_BRANCH == t) {				
						processSwitch(reader, trie, source, sourceMask, hasSafePoint);				
					} else {
						if (TrieParser.TYPE_BRANCH_VALUE == t) {   
							processBinaryBranch(reader, trie, source, sourceLength, sourceMask);				
						} else {
							
							hasSafePoint = extractValue(reader, trie, source, sourceLength, 
									                   sourceMask, hasSafePoint, t, lastType);
						
						}							
					}
				}
			}			
			lastType = t;
		}
		
	}

	private static boolean extractValue(TrieParserReader reader, TrieParser trie, byte[] source, long sourceLength,
			int sourceMask, boolean hasSafePoint, int t, int lastType) {
		if (TrieParser.TYPE_VALUE_BYTES == t) {            	
			parseBytesAction(reader, trie, source, sourceLength, sourceMask, hasSafePoint);	
		} else {
			if (TrieParser.TYPE_VALUE_NUMERIC == t) {
				parseNumericAction(reader, trie, source, sourceLength, sourceMask, hasSafePoint);		
			}  else {
				if (TrieParser.TYPE_SAFE_END == t) {
					hasSafePoint = processSafeEndAction(reader, trie, sourceLength);                                             
				} else  {       
					reportError(reader, trie, lastType);									
				}
			}
		}
		return hasSafePoint;
	}

	
	private static void parseRun(TrieParserReader reader, TrieParser trie, byte[] source, long sourceLength,
			int sourceMask, boolean hasSafePoint) {
		//run
		final int run = trie.data[reader.pos++];    

		//we will not have the room to do a match.
		final boolean temp = !hasSafePoint && 0==reader.altStackPos;
		
		if (reader.runLength+run <= sourceLength || !temp) {

			//TODO: can we know if it NEVER has a safe point and never has alt stack..S
			
			if (temp) {
			
				if (trie.skipDeepChecks) {
					reader.pos += run;
					reader.localSourcePos += run; 
					reader.runLength += run;
					reader.type = trie.data[reader.pos++];
					
				} else {
					//System.out.println("xxxxxxxxxxx");//not called
					scanForRun(reader, trie, source, sourceMask, hasSafePoint, run);
				}
				
			} else {
				//This is called for Headers Trie and the URL Route Trie, both have a single altStackPos since
				//in both cases we want an unknown value to be processed special.
				//System.out.println("yyyyyyyyyyyy called about half the time "+(!hasSafePoint)+" && 0=="+(reader.altStackPos));
				scanForRun(reader, trie, source, sourceMask, hasSafePoint, run);
				
				//System.out.println(trie);
				
			}
			
		} else {
			reader.normalExit=false;
			reader.result = reader.unfoundConstant;
			reader.runLength += run;  			
		}
		
	}
	
	private static void scanForRun(TrieParserReader reader, TrieParser trie, byte[] source, int sourceMask,
			boolean hasSafePoint, final int run) {
	
		if (scanBytes3( reader, source, sourceMask, 
					    trie.caseRuleMask, 
					    reader.pos+run, 
					    reader.localSourcePos+run,
					    trie.data, reader.pos, reader.localSourcePos, run)) {
			reader.runLength += run;
			//System.out.println("run matched now at "+reader.pos);
			reader.type = trie.data[reader.pos++];
		} else {
			//System.out.println("shoud not be called under test...");
			//TODO: this is getting called and rollback as part of branch
			//     it has no side effect but does show up in the profiler...
			noMatchAction(reader, trie, hasSafePoint,
					(reader.alwaysCompletePayloads || (reader.sourceLen >= run))
					 ? reader.noMatchConstant : reader.unfoundConstant);
		}
	}

	private static boolean scanBytes3(TrieParserReader reader, final byte[] source, final int srcMask,
			final byte caseMask, final int t1, final int t2, final short[] data, int t11, int t21, int r) {
		if (t11+r < data.length) {
			int total = 0;
			while (--r >= 0) {		
				
				
//				//repeating this if is probably a bad idea lets do a logic approach instead
//				if ((caseMask & data[t11++]) != (caseMask & 0xFF & source[srcMask & (t21++)]) ) {
//					return false;
//				}				
				
				//xor
				total |= (((caseMask & data[t11++]) ^ (caseMask & 0xFF & source[srcMask & (t21++)]) ));
				
				
			}
			if (total==0) {
				reader.pos = t1;
				reader.localSourcePos = t2;
				return true;
			}
		}
		
		return false;			
	}


	private static void processSwitch(TrieParserReader reader, TrieParser trie, 
			                          byte[] source, int sourceMask, boolean hasSafePoint) {
	
	
			short sourceShort = (short) (trie.caseRuleMask&0xFF&source[sourceMask & reader.localSourcePos]);
			
			assert(TrieParser.TYPE_SWITCH_BRANCH == trie.data[reader.pos-1]);
			
			int p = reader.pos;
			int metaPos = p++;
			short metaData = trie.data[metaPos];
			//Also needed when we grow the switch on insert later
			switchJump(reader, trie, hasSafePoint, p, trie.data, metaPos, 
					  (short)(metaData & 0xFF), sourceShort-(short)((metaData>>8) & 0xFF));
		
	}

	private static void switchJump(TrieParserReader reader, TrieParser trie, boolean hasSafePoint, int p,
			short[] localData, final int metaPos, final short trieLen, int len) {
		
		if ((len >= 0) && ( len < trieLen )) {					
			final int pJump = p+(len<<1);
			switchJumpImpl(reader, trie, hasSafePoint, localData, metaPos, trieLen, pJump, (int)localData[pJump]);
		} else {
			//System.out.println("no jump");
			noMatchAction(reader, trie, hasSafePoint, reader.noMatchConstant);
		}
	}

	private static void switchJumpImpl(TrieParserReader reader, TrieParser trie, boolean hasSafePoint,
			short[] localData, final int metaPos, final short trieLen, final int pJump, int topVal) {
		
		if (topVal >= 0) {				
			
			//jump to new position, all are relative to the end of the jump table so no values need to be
			//adjusted if the jump table grows with new inserts.
			int p = ((topVal<<15) | (0x7FFF&localData[pJump+1]))+(metaPos+(trieLen<<1));

			//read next type and restore the reader position
			reader.type = localData[p++];
			assert(reader.type<8 && reader.type>=0) : "bad type:"+reader.type;
			reader.pos = p;
			//System.out.println("jumped to new position: "+p);
		} else {
			noMatchAction(reader, trie, hasSafePoint, reader.noMatchConstant);
		}
	}
	
	
	private static void processBinaryBranch(TrieParserReader reader,
			TrieParser trie, byte[] source, long sourceLength,
			int sourceMask) {
		
		if (reader.runLength < sourceLength) {
			processMultipleBinBranches(reader, (short) source[sourceMask & reader.localSourcePos], reader.pos, trie.data);
		} else {
			reader.normalExit = false;
			reader.result = reader.unfoundConstant;
		}
		
	}

	private static void processMultipleBinBranches(TrieParserReader reader,
			final short sourceShort, int p,
			final short[] localData) {
		
		p = (0==(TrieParser.computeJumpMask(sourceShort, localData[p])&0xFFFFFF))
			? p+3 
			: p+3+((((int)localData[p+1])<<15) | (0x7FFF&localData[p+2]));			
						
		reader.type = localData[p++];
		reader.pos = p;
	}


	private static void parseNumericAction(TrieParserReader reader, TrieParser trie, byte[] source,
			final long sourceLength, int sourceMask, boolean hasSafePoint) {
		if (reader.runLength<sourceLength) {
			if ((reader.localSourcePos = parseNumeric(trie.ESCAPE_BYTE, reader,source,reader.localSourcePos, sourceLength-reader.runLength, sourceMask, trie.data[reader.pos++]))<0) {			            	
				
				noMatchAction(reader, trie, hasSafePoint, reader.noMatchConstant);

			} else {
				//finished parse of number so move next
				reader.type = trie.data[reader.pos++];
			}
		} else {
		
			noMatchAction(reader, trie, hasSafePoint, reader.unfoundConstant);

		}
	}

	private static boolean processSafeEndAction(TrieParserReader reader, TrieParser trie, final long sourceLength) {
		boolean hasSafePoint;
		recordSafePointEnd(reader, reader.localSourcePos, reader.pos, trie);  
		hasSafePoint = true;
		reader.pos += trie.SIZE_OF_RESULT;
		if (sourceLength == reader.runLength) {
			reader.normalExit=false;
			reader.result = useSafePointNow(reader);
		} else {
			//move next since we did not take the safe point
			reader.type = trie.data[reader.pos++];
		}
		return hasSafePoint;
	}

	private static void parseBytesAction(final TrieParserReader reader, final TrieParser trie, final byte[] source,
			final long sourceLength, final int sourceMask, boolean hasSafePoint) {

		if (( ((sourceLength >= reader.runLength )) 
				&&  
				(parseBytes(reader, trie, source, sourceLength, sourceMask)))) {
			
			//move next since we did not need to exit
			reader.type = trie.data[reader.pos++];
			
		} else {
			noMatchAction(reader, trie, hasSafePoint, reader.unfoundConstant);		
		}
	}

	private static void noMatchAction(final TrieParserReader reader, 
			final TrieParser trie, boolean hasSafePoint,
			final long result) {
		/////////////////

		//common pattern
		if (!hasSafePoint) {
			if (reader.altStackPos <= 0) {                                
				//we have NO safe point AND we found a non match in the sequence
				//this will never match no matter how much data is added so return the noMatch code.
				reader.normalExit=false;
				reader.result = result;
			} else {
				reader.altStackPos = loadupNextChoiceFromStack(reader, trie.data, reader.altStackPos);                           
			}
		} else {
			
			reader.normalExit=false;
			reader.result = useSafePoint(reader);
			
		}
		///////////////
	}

	private static void reportError(TrieParserReader reader, TrieParser trie, int lastType) {
		logger.error(trie.toString());
		throw new UnsupportedOperationException("Bad jump length now at position "+(reader.pos-1)+" type found "+reader.type+" previous valid type "+lastType);
	}

	private static boolean parseBytes(final TrieParserReader reader, final TrieParser trie, final byte[] source, final long sourceLength, final int sourceMask) {

		short[] localWorkingMultiStops = reader.workingMultiStops;

		int localRunLength = reader.runLength;
		long maxCapture = sourceLength-localRunLength;
		final int localSourcePos = reader.localSourcePos;
		int localCaputuredPos = reader.capturedPos;


		if (maxCapture>0) {
			short stopValue = trie.data[reader.pos++];

			int stopCount = 0;

			int[] localWorkingMultiContinue = reader.workingMultiContinue;

			localWorkingMultiContinue[stopCount] = reader.pos;
			localWorkingMultiStops[stopCount++] = stopValue;

			if (reader.altStackPos==0) {
			}else {
				stopCount = scanAltStack(reader, trie, localWorkingMultiStops, 
						localRunLength, localSourcePos,
						localCaputuredPos, stopCount, 
						localWorkingMultiContinue);
			}	

			if (stopCount<=1) {				
				return -1 != (reader.localSourcePos = parseBytes(reader,source,reader.localSourcePos, maxCapture, sourceMask, stopValue));
			} else {
				return multiStopCount(reader, source, sourceMask, 
						localWorkingMultiStops, maxCapture, localSourcePos,
						stopValue, stopCount);
			}
		} else {
			reader.localSourcePos = -1;
			return false;
		}
	}

	private static boolean multiStopCount(final TrieParserReader reader, final byte[] source, final int sourceMask,
			short[] localWorkingMultiStops, long maxCapture, final int localSourcePos, short stopValue, int stopCount) {
		assert(localWorkingMultiStops.length>0);
		int x = localSourcePos;
		int lim = maxCapture<=sourceMask ? (int)maxCapture : sourceMask+1;	        	

		if (stopCount==2 && stopValue!=0) {
			//special case since this happens very often

			final short s1 = localWorkingMultiStops[0];
			final short s2 = localWorkingMultiStops[1]; //B DEBUG CAPTURE:keep-alive  B DEBUG CAPTURE:127.0.0.1

			do { 
				short value = source[sourceMask & x++];
				if (value==s2) {
					reader.pos = reader.workingMultiContinue[1];
					return assignParseBytesResults(reader, sourceMask, localSourcePos, x);							
				} else if (value==s1) {
					reader.pos = reader.workingMultiContinue[0];
					return assignParseBytesResults(reader, sourceMask, localSourcePos, x);						
				}

			} while (--lim > 0); 

			reader.localSourcePos =-1;

			return false;

		} else {
			int stopIdx = -1;
			do {  
			} while ( (-1== (stopIdx=indexOfMatchInArray(source[sourceMask & x++], localWorkingMultiStops, stopCount ))) && (--lim > 0));
			return assignParseBytesResults(reader, sourceMask, localSourcePos, x, stopIdx);
		}
	}

	private static int scanAltStack(final TrieParserReader reader, final TrieParser trie,
			short[] localWorkingMultiStops, int localRunLength, final int localSourcePos, int localCaputuredPos,
			int stopCount, int[] localWorkingMultiContinue) {
		short[] localData = trie.data;

		int i = reader.altStackPos; 
		int[] localAltStack = reader.altStack;

		while (--i>=0) {
			int base = i*fieldsOnStack;
			int cTemp = localAltStack[base+2];
			if (localData[cTemp] == TrieParser.TYPE_VALUE_BYTES) {

				if (localCaputuredPos != localAltStack[base+1]) {//part of the same path.
					break;
				}
				if (localSourcePos != localAltStack[base+0]) {//part of the same path.
					break;
				}
				if (localRunLength != localAltStack[base+3]){
					break;
				}

				//ensure newStop is not already in the list of stops.				       
				short newStop = localData[cTemp+1];
				if (-1 != indexOfMatchInArray(newStop, localWorkingMultiStops, stopCount)) {               
					break;
				}

				localWorkingMultiContinue[stopCount] = cTemp+2;
				localWorkingMultiStops[stopCount++] = newStop;

				//taking this one
				reader.altStackPos = i;

			}
		}
		return stopCount;
	}

	private static boolean assignParseBytesResults(final TrieParserReader reader, final int sourceMask,
			final int localSourcePos, int x) {
		int len = (x-localSourcePos)-1;
		reader.runLength += (len);

		reader.capturedPos = extractedBytesRange(reader.sourceBacking, reader.capturedValues, reader.capturedPos, localSourcePos, len, sourceMask);  
		reader.localSourcePos = x;
		return true;
	}

	private static boolean assignParseBytesResults(TrieParserReader reader, int sourceMask, final int sourcePos, int x, int stopIdx) {

		//this is for the case where we match up to the very end of the string		
		if (reader.alwaysCompletePayloads && -1 == stopIdx) {
			int j = reader.workingMultiStops.length;
			while (--j>=0) {
				if (reader.workingMultiStops[j]==0) {
					stopIdx = j;
				}
			}
		}

		if (-1==stopIdx) {//not found!
			reader.localSourcePos =-1;
			return false;
		} else {
			int len = (x-sourcePos)-1;
			reader.runLength += (len);

			reader.capturedPos = extractedBytesRange(reader.sourceBacking ,reader.capturedValues, reader.capturedPos, sourcePos, len, sourceMask);  
			reader.localSourcePos = x;
			reader.pos = reader.workingMultiContinue[stopIdx];
			return true;
		}
	}

	private static void initForQuery(TrieParserReader reader, TrieParser trie, 
			                         byte[] source, int sourcePos, int sourceMask, long unfoundResult, long noMatchResult) {

		assert(trie.getLimit()>0) : "SequentialTrieParser must be setup up with data before use.";
		
		reader.capturedPos = 0;
		reader.sourceBacking = source;
		
		//working vars
		reader.runLength = 0;
		reader.localSourcePos = sourcePos;
		
		reader.result = unfoundResult;
		reader.unfoundConstant = unfoundResult;
		reader.noMatchConstant = noMatchResult;
		
		reader.normalExit = true;
		reader.altStackPos = 0; 
		
		lazyInitCapturedArray(reader, trie);
	
		reader.sourceMask = Branchless.ifZero(reader.sourceMask, sourceMask, reader.sourceMask);


	}

	private static void lazyInitCapturedArray(TrieParserReader reader, TrieParser trie) {
		//only allocate when this is going to be used.
		if (TrieParser.maxExtractedFields(trie) > 0) {
			int len = (1+TrieParser.maxExtractedFields(trie))<<4;
			if (reader.capturedValuesLength<len) {
				reader.capturedValues = new int[len];
				reader.capturedValuesLength = reader.capturedValues.length;
			}
		}
	}

	
	private static void processAltBranch(TrieParserReader reader, byte[] source, short[] localData, boolean hasSafePoint) {
		assert(localData[reader.pos]>=0): "bad value "+localData[reader.pos];
		assert(localData[reader.pos+1]>=0): "bad value "+localData[reader.pos+1];

		//the extracted (byte or number) is ALWAYS local so push LOCAL position on stack and take the JUMP        	

		int pos = reader.pos;
		int nearJump = pos + TrieParser.BRANCH_JUMP_SIZE;
		int farJump = pos + ((((int)localData[pos])<<15) | (0x7FFF&localData[1+pos]))+ TrieParser.BRANCH_JUMP_SIZE;
		
		//if local is NOT numeric then take the jump first
		if (localData[nearJump] != TrieParser.TYPE_VALUE_NUMERIC) {

			//push local on stack so we can try the captures if the literal does not work out. (NOTE: assumes all literals are found as jumps and never local)
			reader.altStackPos = pushAlt(reader.altStack, 
					                     reader.localSourcePos, 
					                     reader.capturedPos, 
					                     nearJump, 
					                     reader.runLength, 
					                     reader.altStackPos);
					
			pos = farJump;
		} else {
			
			reader.altStackPos = pushAlt(reader.altStack, 
                    reader.localSourcePos, 
                    reader.capturedPos, 
                    farJump, 
                    reader.runLength, 
                    reader.altStackPos);

			pos = nearJump;
		}
		reader.type=localData[pos++];
		reader.pos = pos;		
	}

	private static long useSafePointNow(TrieParserReader reader) {
		//hard stop passed in forces us to use the safe point
		reader.sourceLen -= (reader.localSourcePos -reader.sourcePos);
		reader.sourcePos = reader.localSourcePos;
		return reader.safeReturnValue;
	}

	private static int loadupNextChoiceFromStack(TrieParserReader reader, short[] localData, int altStackPos) {
		//try other path
		//reset all the values to the other path and continue from the top

		int base = --altStackPos * fieldsOnStack;
		
		reader.localSourcePos     = reader.altStack[base+0];
		reader.capturedPos        = reader.altStack[base+1];
		int p        	  	      = reader.altStack[base+2];
		reader.runLength 		  = reader.altStack[base+3];                                

		reader.type = localData[p];
		reader.pos = 1+p;
		return altStackPos;
	}

	private static long useSafePoint(TrieParserReader reader) {
		reader.localSourcePos = reader.safeSourcePos;
		reader.capturedPos = reader.safeCapturedPos;
		reader.sourceLen = reader.saveCapturedLen;
		reader.sourcePos =reader.localSourcePos;

		return reader.safeReturnValue;
	}

	static int pushAlt(int[] altStack, int offset, int capPos, int pos, int runLength, int altStackPos) {
		int base = fieldsOnStack*altStackPos++;
		altStack[base++] = offset;
		altStack[base++] = capPos;
		altStack[base++] = pos;        
		altStack[base] = runLength;		
		return altStackPos;
	}

	private static void recordSafePointEnd(TrieParserReader reader, int localSourcePos, int pos, TrieParser trie) {

		reader.safeReturnValue = TrieParser.readEndValue(trie.data, pos, trie.SIZE_OF_RESULT);
		reader.safeCapturedPos = reader.capturedPos;
		reader.saveCapturedLen = reader.sourceLen;

		reader.safeSourcePos = localSourcePos;        


		//if the following does not match we will return this safe value.
		//we do not yet have enough info to decide if this is the end or not.
	}
	
	private static int parseBytes(TrieParserReader reader, 
			                      final byte[] source, final int sourcePos, 
			                      final long remainingLen, 
			                      final int sourceMask, final short stopValue) {              
				
		int x = sourcePos;
		int lim = remainingLen<=sourceMask ? (int)remainingLen : sourceMask+1;
	
		do {
		} while ( ((stopValue!=source[sourceMask & x++])) && (--lim > 0));         

		final boolean hasStopValue = 0!=stopValue;
		if (!((lim<=0) && hasStopValue)) { 
			
			final int x1 = hasStopValue ? x : x+1;
			final int len = (x1-sourcePos)-1;
			
			//final int len = hasStopValue ? (x-sourcePos)-1 : x-sourcePos; 

//			if (len>0) {
////				OK  -<CAC
////				date  -<CAC
////				Tue, 04 Dec 2018 05:17:24 GMT  -<CAC
////				server  -<CAC
////				GreenLightning  -<CAC
////				application/json  -<CAC
//				
//				Appendables.appendUTF8(System.out, source, sourcePos, len, sourceMask);
//				System.out.println("  -<CAC");
//				
//			}
		
			
			reader.runLength += (len);
			reader.capturedPos = extractedBytesRange(reader.sourceBacking, 
					reader.capturedValues, 
					reader.capturedPos, 
					sourcePos, len, sourceMask);                
			return x1;//if no stop value add 1 more since stop is subtracted			
		} else {
			//a zero stop value is a rule to caputure evertything up to the end of the data.
			return -1;//not found!
		}
	}

	private static int indexOfMatchInArray(short value, short[] data, int i) {
		if (1==i) {
			return Branchless.ifEquals(value, data[0], 0, -1);
			//return (value==data[0]) ? 0 : -1;
		}
		return indexOfMatchInArrayScan(value, data, i);
	}

	private static int indexOfMatchInArrayScan(short value, short[] data, int i) {
		while (--i>=0) {
			if (value == data[i]) {
				return i;
			}
		}
		return -1;
	}

	private static int extractedBytesRange(byte[] backing, int[] target, int pos, int sourcePos, int sourceLen, int sourceMask) {
		try {

			//    		Appendables.appendUTF8(System.out, backing, sourcePos, sourceLen, sourceMask);
			//  		    System.out.println();		

			target[pos++] = 0;  //this flag tells us that these 4 values are not a Number but instead captured Bytes
			target[pos++] = sourcePos;
			target[pos++] = sourceLen;
			target[pos++] = sourceMask;
			return pos;

		} catch (ArrayIndexOutOfBoundsException e) {
			throw new UnsupportedOperationException("TrieParserReader attempted to capture too many values. "+(pos/4));
		}
	}

	private static int parseNumeric(final byte escapeByte, TrieParserReader reader, 
			                         byte[] source, int sourcePos, 
			                         long sourceLengthIn, int sourceMask, short numTypeIn) {

		//////////////support for fixed length numbers up to 1024
		final int fixedLength = (NUMERIC_LENGTH_MASK&(numTypeIn>>>NUMERIC_LENGTH_SHIFT));
		assert(fixedLength == 0) : "Not yet implemented";
		final boolean templateLimited = (fixedLength>0 && fixedLength<=sourceLengthIn);
		return parseNumericImpl(
				escapeByte, 
				reader, 
				source, 
				sourcePos, 
				sourceMask, 
				0!=(NUMERIC_ABSENT_IS_ZERO_MASK&numTypeIn), 
				(short)(numTypeIn & NUMERIC_TYPE_MASK),
				templateLimited, 
				templateLimited ? fixedLength : sourceLengthIn, 
				(short) source[sourceMask & sourcePos]);
	}

	private static int parseNumericImpl(final byte escapeByte, final TrieParserReader reader, final byte[] source, final int sourcePos,
			final int sourceMask, final boolean absentIsZero, final short numType, final boolean templateLimited, final long sourceLength,
			final short c1) {
		
		if (escapeByte != c1) {
			
			//this is the most common case, normal unsigned integers
			if (0 == ((TrieParser.NUMERIC_FLAG_DECIMAL|TrieParser.NUMERIC_FLAG_RATIONAL|TrieParser.NUMERIC_FLAG_SIGN) & numType) ) {
				
				return parseNumericImpl(reader, source, sourcePos, 
						sourceLength, 
						sourceMask,
						numType, 
						absentIsZero, 
						templateLimited,
						(byte) 1, (long) 0, (byte) 0, 0);
			} else {			
			
				return parseNumericSlow(reader, source, sourcePos, 
						sourceLength, sourceMask, 
						numType, absentIsZero,
						templateLimited, (byte) 1, (long) 0, (byte) 0, 0, c1);
			}			
			
		} else {
			return lteralNumericPatternMatch(source, sourcePos, sourceMask, numType);
		}
	}

	private static int lteralNumericPatternMatch(byte[] source, int sourcePos, int sourceMask, short numType) {
		//////////////////////////////////////////////////////////////
		//This is for supporting %i as an actual value to match that pattern rather than a number

		sourcePos++;
		final int typeMask = TrieParser.buildNumberBits(source[sourceMask & sourcePos]);
		sourcePos++;
		return ((typeMask&numType)==typeMask) ? sourcePos : -1;
	}

	private static int parseNumericSlow(TrieParserReader reader, byte[] source, int sourcePos, long sourceLength,
			int sourceMask, short numType, final boolean absentIsZero, final boolean templateLimited, byte sign,
			long intValue, byte intLength, int dot, final short c1) {
		
		// dot is  only set to one for NUMERIC_FLAG_DECIMAL 
		
		if (0!= (TrieParser.NUMERIC_FLAG_DECIMAL&numType)) {
			//support for decimals
			dot=1;
			if ('.'!=c1) {
				publish(reader, 1, 0, 1, 10, dot);
				//do not parse numeric
				return sourcePos;
			} else {
				sourcePos++;
			}
			
		} else if (0!= (TrieParser.NUMERIC_FLAG_RATIONAL&numType)) {
			//logger.info("parse rational");
			//support of rational
			if ('/'!=c1) {
				publish(reader, 1, 1, 1, 10, dot);
				//do not parse numeric
				return sourcePos;
			} else {
				sourcePos++;
			}
			
		}
		
		//NOTE: these Numeric Flags are invariants consuming runtime resources, this tree could be pre-compiled to remove them if neded.
		if (0!=(TrieParser.NUMERIC_FLAG_SIGN&numType)) {
			//logger.info("parse signed");
			//support for signed ints
			if (c1=='-') { //NOTE: check ASCII table there may be a faster way to do this.
				sign = -1;
				sourcePos++;
			} else if (c1=='+') {
				sourcePos++;
			}
		}
		
		return parseNumericImpl(reader, source, sourcePos, sourceLength, 
				sourceMask, numType, absentIsZero, templateLimited,
				sign, intValue, intLength, dot);
	}

	private static int parseNumericImpl(TrieParserReader reader, byte[] source, int sourcePos, long sourceLength,
			int sourceMask, short numType, final boolean absentIsZero, final boolean templateLimited, byte sign,
			long intValue, byte intLength, int dot) {
		
		if ((  ('x'!=source[sourceMask & sourcePos+1]) || ('0'!=source[sourceMask & sourcePos+0])) 
			&& 0==(TrieParser.NUMERIC_FLAG_HEX&numType) ) {    
			return parseBaseTenImpl(reader, source, 
					sourcePos, sourceLength, sourceMask, absentIsZero, templateLimited,
					sign, intValue, intLength, dot);
			
		} else {
			return parseBaseHexImpl(reader, source, sourcePos, sourceLength, sourceMask, absentIsZero, templateLimited,
					sign, intValue, intLength, dot, ('0'!=source[sourceMask & sourcePos+0]) || ('x'!=source[sourceMask & sourcePos+1]));
		}


	}

	private static int parseBaseHexImpl(TrieParserReader reader, byte[] source, int sourcePos, long sourceLength,
			int sourceMask, final boolean absentIsZero, final boolean templateLimited, byte sign, long intValue,
			byte intLength, int dot, boolean hasNo0xPrefix) {
		byte base;
		//just to keep it from spinning on values that are way out of bounds
		sourceLength = Math.min(LONGEST_LONG_HEX_DIGITS+1, sourceLength); //never scan over 32

		base = 16;
		if (!hasNo0xPrefix) {
			sourcePos+=2;//skipping over the 0x checked above
		}
		short c = 0;
		do {
			c = source[sourceMask & sourcePos++];

			if (intLength<sourceLength) {

				if ((c>='0') && (c<='9') ) {
					intValue = (intValue<<4)+(c-'0');
					intLength++;
					continue;
				} else  {
					c = (short)(c | 0x20);//to lower case
					if ((c>='a') && (c<='f') ) {
						intValue = (intValue<<4)+(10+(c-'a'));
						intLength++;
						continue;
					} else {
						//this is not a valid char so we reached the end of the number
						break;
					}
				}
			} else {
				if (reader.alwaysCompletePayloads || templateLimited) {
					//do not reset the length;
				} else {
					//we are waiting for more digits in the feed. 
					// intLength>=sourceLength
					intLength=0;
				}
				break;
			}
		}  while (true);
		return parseBaseTenFinish(reader, sourcePos, absentIsZero, sign, intValue, intLength, dot, base);
	}

	private static int parseBaseTenImpl(TrieParserReader reader, byte[] source, int sourcePos, final long sourceLengthIn,
			int sourceMask, final boolean absentIsZero, final boolean templateLimited, byte sign, long intValue,
			byte intLength, int dot) {
		//just to keep it from spinning on values that are way out of bounds
		final long sourceLength = Math.min(LONGEST_LONG_DIGITS+1, sourceLengthIn); //never scan over 32

		do {

			if (intLength < sourceLength) {
				final short c = source[sourceMask & sourcePos++];        

				if ((c>='0') && (c<='9') ) {
					intValue = (intValue * 10)+(c & 0xF);
					intLength++;
					continue;
				} else {
					break;//next char is not valid.
				}
			} else {
				if (reader.alwaysCompletePayloads || templateLimited) {
					break;
				} else {
					return -1; //we are waiting for more digits in the feed. 
				}
			}

		}  while (true);
		return parseBaseTenFinish(reader, sourcePos, absentIsZero, sign, intValue, intLength, dot, (byte) 10);
	}

	private static int parseBaseTenFinish(TrieParserReader reader, int sourcePos, final boolean absentIsZero, byte sign,
			long intValue, byte intLength, int dot, byte base) {
		if (intLength==0 && !absentIsZero) {
			return -1;
		}
		publish(reader, sign, intValue, intLength, base, dot);
		return sourcePos-1;
	}

	private static void publish(TrieParserReader reader, int sign, long numericValue, int intLength, int base, int isDot) {
		assert(0!=sign);

		reader.capturedValues[reader.capturedPos++] = sign;
		reader.capturedValues[reader.capturedPos++] = (int) (numericValue >> 32);
		reader.capturedValues[reader.capturedPos++] = (int) (0xFFFFFFFF & numericValue);

		assert(base<=64 && base>=2);
		assert(isDot==1 || isDot==0);

		reader.capturedValues[reader.capturedPos++] = (isDot<<31) | (base<<16) | (0xFFFF & intLength) ; //Base: 10 or 16, IntLength:  

	}

	public static void setCapturedShort(TrieParserReader reader, int idx, int unsignedShort) {
		int pos = idx*4;
		if (null==reader.capturedValues || pos>=reader.capturedValues.length) {
			int[] newInt = new int[4*(1+idx)*4];
			if (reader.capturedValues!=null) {
				System.arraycopy(reader.capturedValues, 0, newInt, 0, reader.capturedValues.length);
			}
			reader.capturedValues = newInt;
		}
		reader.capturedValues[pos++] = unsignedShort>=0 ? 1 : -1;
		reader.capturedValues[pos++] = 0;
		reader.capturedValues[pos++] = unsignedShort;
		reader.capturedValues[pos] = 0;		
	}
	
	public static void writeCapturedShort(TrieParserReader reader, int idx, DataOutput target) {
				
		int pos = idx*4;

		
		int sign = reader.capturedValues[pos++];
		assert(sign!=0);
		
		pos++;//skip high since we are writing a short
		try {
			target.writeShort((short)reader.capturedValues[pos++]);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}        
	}

	public static int writeCapturedUTF8(TrieParserReader reader, int idx, ChannelWriter target) {
		int pos = idx*4;

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int p = reader.capturedValues[pos++];
		int l = reader.capturedValues[pos++];
		int m = reader.capturedValues[pos++];

		//this data is already encoded as UTF8 so we do a direct copy
		target.writeShort(l);
		DataOutputBlobWriter.write((DataOutputBlobWriter<?>) target, reader.sourceBacking, p, l, m);
		return l;
	}

	public static void parseSetup(TrieParserReader trieReader, int loc, Pipe<?> input) {

		parseSetup(trieReader, PipeReader.readBytesBackingArray(input, loc), 
				PipeReader.readBytesPosition(input, loc), 
				PipeReader.readBytesLength(input, loc), 
				PipeReader.readBytesMask(input, loc));
	}

	public <T extends ChannelReader> void parseSetup(T reader) {
		parseSetup(this, (DataInputBlobReader<?>)reader);
	}

	public <T extends ChannelReader> void parseSetup(T reader, int length) {
		parseSetup(this, (DataInputBlobReader<?>)reader, length);
	}

	public static <S extends MessageSchema<S>> void parseSetup(TrieParserReader trieReader, 
			DataInputBlobReader<S> reader) {
		DataInputBlobReader.setupParser(reader, trieReader);
	}

	public static <S extends MessageSchema<S>> void parseSetup(TrieParserReader trieReader, 
			DataInputBlobReader<S> reader, 
			int length) {   	    	
		DataInputBlobReader.setupParser(reader, trieReader, length);
	}

	public static void parseSetup(TrieParserReader trieReader, Pipe<?> input) {
		//TODO: cofirm this field is next...
		int meta = Pipe.takeByteArrayMetaData(input);
		int length    = Pipe.takeByteArrayLength(input);
		parseSetup(trieReader, Pipe.byteBackingArray(meta, input), Pipe.bytePosition(meta, input, length), length, Pipe.blobMask(input));
	}

	public static int capturedFieldCount(TrieParserReader reader) {
		return reader.capturedPos>>2;
	}

	public static void capturedFieldInts(TrieParserReader reader, int idx, int[] targetArray, int targetPos) {

		int pos = idx*4;
		assert(pos < reader.capturedValues.length) : "Either the idx argument is too large or TrieParseReader was not constructed to hold this many fields";

		int type = reader.capturedValues[pos++];
		assert(type!=0);
		targetArray[targetPos++] = type;
		targetArray[targetPos++] = reader.capturedValues[pos++];
		targetArray[targetPos++] = reader.capturedValues[pos++];
		targetArray[targetPos++] = reader.capturedValues[pos++];

	}

	public static int capturedFieldBytes(TrieParserReader reader, int idx, byte[] target, int targetPos, int targetMask) {

		int pos = idx*4;
		assert(pos < reader.capturedValues.length) : "Either the idx argument is too large or TrieParseReader was not constructed to hold this many fields";

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int p = reader.capturedValues[pos++];
		int l = reader.capturedValues[pos++];
		int m = reader.capturedValues[pos++];

		Pipe.copyBytesFromToRing(reader.sourceBacking, p, m, target, targetPos, targetMask, l);

		return l;
	}
	
	public static boolean capturedFieldBytesEquals(TrieParserReader reader, int idx, byte[] target, int targetPos, int targetMask) {

		int pos = idx*4;
		assert(pos < reader.capturedValues.length) : "Either the idx argument is too large or TrieParseReader was not constructed to hold this many fields";

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int p = reader.capturedValues[pos++];
		int l = reader.capturedValues[pos++];
		int m = reader.capturedValues[pos++];

		if (l<=target.length) {
			return Pipe.isEqual(reader.sourceBacking, p, m, target, targetPos, targetMask, l);			
		} else {
			return false;
		}

	}

	
	public static int capturedFieldByte(TrieParserReader reader, int idx, int offset) {

		int pos = idx*4;
		assert(pos < reader.capturedValues.length) : "Either the idx argument is too large or TrieParseReader was not constructed to hold this many fields";

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int p = reader.capturedValues[pos++];
		int l = reader.capturedValues[pos++];
		int m = reader.capturedValues[pos++];

		if (offset<l) {
			return 0xFF & reader.sourceBacking[m & (p+offset)];            
		} else {
			return -1;
		}
	}


	public static int capturedFieldBytes(TrieParserReader reader, int idx, ByteConsumer target) {
		assert(null!=reader);
		assert(null!=target);
		int pos = idx*4;
		assert(pos < reader.capturedValues.length) : "Either the idx argument is too large or TrieParseReader was not constructed to hold this many fields";

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int bpos = reader.capturedValues[pos++];
		int blen = reader.capturedValues[pos++];
		int bmsk = reader.capturedValues[pos++];

		try {
			target.consume(reader.sourceBacking, bpos, blen, bmsk);
		} catch (Throwable t) {
			t.printStackTrace();
		}
		return blen;

	}

	public static int capturedFieldBytesLength(TrieParserReader reader, int idx) {
		assert(null!=reader);

		int pos = idx*4;
		assert(pos < reader.capturedValues.length) : "Either the idx argument is too large or TrieParseReader was not constructed to hold this many fields";
		return reader.capturedValues[2+pos];

	}

	public static long capturedFieldQuery(TrieParserReader reader, int idx, TrieParserReader reader2, TrieParser trie) {
		//two is the default for the stop bytes.
		return capturedFieldQuery(reader,idx,reader2,2,trie);
	}

	//parse the capture text as a query against yet another trie
	public static <A extends Appendable> long capturedFieldQuery(TrieParserReader reader, int idx, TrieParserReader reader2, int stopBytesCount, TrieParser trie) {

		int pos = idx*4;
		assert(pos < reader.capturedValues.length) : "Either the idx argument is too large or TrieParseReader was not constructed to hold this many fields";

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int bpos = reader.capturedValues[pos++];
		int blen = reader.capturedValues[pos++];
		int bmsk = reader.capturedValues[pos++];

		//we add 2 to the length to pick up the stop chars, this ensure we have enough text to match
		return query(reader2, trie, reader.sourceBacking, bpos, blen+stopBytesCount, bmsk, -1);

	}

	public static void capturedFieldSetValue(TrieParserReader reader, int idx, TrieParser trie, long value) {

		int pos = idx*4;
		assert(pos < reader.capturedValues.length) : "Either the idx argument is too large or TrieParseReader was not constructed to hold this many fields";

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int bpos = reader.capturedValues[pos++];
		int blen = reader.capturedValues[pos++];
		int bmsk = reader.capturedValues[pos++];

		trie.setValue(reader.sourceBacking, bpos, blen, bmsk, value);

	}

	public static void clearCapturedBytes(TrieParserReader reader, int idx) {
		int pos = idx*4;
		if (null==reader.capturedValues || pos>=reader.capturedValues.length) {
			return;
		}
		reader.capturedValues[pos++] = 0;
		reader.capturedValues[pos++] = 0;
		reader.capturedValues[pos++] = 0;
		reader.capturedValues[pos] = 0;		
	}
	
	public static boolean hasCapturedBytes(TrieParserReader reader, int idx) {
		int pos = idx*4;
				
		return (pos<reader.capturedPos)
				&& 0!=reader.capturedValues[pos]
				&& reader.capturedValues[pos+2]>=0;	
 	
	}
	
	
	public static <A extends Appendable> A capturedFieldBytesAsUTF8(TrieParserReader reader, int idx, A target) {

		int pos = idx*4;
		assert(pos < reader.capturedValues.length) : "Either the idx argument is too large or TrieParseReader was not constructed to hold this many fields";

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int bpos = reader.capturedValues[pos++];
		int blen = reader.capturedValues[pos++];
		int bmsk = reader.capturedValues[pos++];

		return Appendables.appendUTF8(target, reader.sourceBacking, bpos, blen, bmsk);

	}

	public static <A extends Appendable> A capturedFieldBytesAsUTF8Debug(TrieParserReader reader, int idx, A target) {

		int pos = idx*4;
		assert(pos < reader.capturedValues.length) : "Either the idx argument is too large or TrieParseReader was not constructed to hold this many fields";

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int bpos = reader.capturedValues[pos++];
		int blen = reader.capturedValues[pos++];
		int bmsk = reader.capturedValues[pos++];

		return Appendables.appendUTF8(target, reader.sourceBacking, bpos-10, blen+20, bmsk);

	}

	public static int writeCapturedUTF8ToPipe(TrieParserReader reader, Pipe<?> target, int idx, int loc) {
		int pos = idx*4;

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int bpos = reader.capturedValues[pos++];
		int blen = reader.capturedValues[pos++];
		PipeWriter.writeBytes(target, loc, reader.sourceBacking, bpos, blen, reader.capturedValues[pos++]);

		return blen;

	}

	public static <S extends MessageSchema<S>> int writeCapturedValuesToDataOutput(TrieParserReader reader, DataOutputBlobWriter<S> target) {
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		//NOTE: this method is used by the HTTP1xRouterStage class to write all the captured fields which is key to GreenLightning
		//      ensure that any changes here are matched by the methods consuming this DataOutput inside GreenLightnining.
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		int limit = reader.capturedPos;
		int[] localCapturedValues = reader.capturedValues;

		int totalBytes = 0;
		int i = 0;
		while (i < limit) {

			int type = localCapturedValues[i++];

			int writePosition = target.position();

			if (isCapturedByteData(type)) {

				int p = localCapturedValues[i++];
				int l = localCapturedValues[i++];
				int m = localCapturedValues[i++];   

				totalBytes += l;

				//       logger.info("captured text: {}", Appendables.appendUTF8(new StringBuilder(), reader.capturedBlobArray, p, l, m));

				//if those bytes were utf8 encoded then this matches the same as writeUTF8 without decode/encode                
				target.writeShort(l); //write the bytes count as a short first, then the UTF-8 encoded string
				DataOutputBlobWriter.write(target,reader.sourceBacking,p,l,m);

			} else {

				int sign = type;
				long value1 = localCapturedValues[i++];
				long value2 = localCapturedValues[i++]; 

				int meta = localCapturedValues[i++]; 
				boolean isDot = (meta<0);//if high bit is on this is a dot value
				byte base = (byte)((meta>>16)&0xFF);
				//int len  = meta&0xFFFF;

				long value = sign*((value1<<32)|value2);
				if (isDot) {
					if (base!=10) {
						throw new UnsupportedOperationException("Does support decimal point values with hex, please use base 10 decimal.");
					}
				} else {
					int position = 0;

					//Jump ahead to combine the dot part of the number if it is found.
					if (i+4<=limit //if there is following data
							&& (!isCapturedByteData(localCapturedValues[i])) //if next data is some kind of number	
							&& (localCapturedValues[i+3]<0)) { //if that next data point is the second half

						//decimal value                			
						//grab the dot value and roll it in.
						int dsign = localCapturedValues[i++];
						long dvalue1 = localCapturedValues[i++];
						long dvalue2 = localCapturedValues[i++];                    
						int dmeta = localCapturedValues[i++];

						byte dbase = (byte)((dmeta>>16)&0xFF);
						if (dbase!=10) {
							throw new UnsupportedOperationException("Does support decimal point values with hex, please use base 10 decimal.");
						}

						if (0 != position) {
							throw new UnsupportedOperationException("Expected left side of . to be a simple integer.");
						}

						int dlen  = dmeta&0xFFFF;

						long dvalue = dsign*((dvalue1<<32)|dvalue2);

						//shift the integer part up and add the decimal part
						value = (value*Decimal.longPow[dlen])+dvalue;

						//modify position to have the right number of points
						position = -dlen;  

						target.writePackedLong(value);
				
						//write second part and it gets its own entry.
						writePosition = target.position();                		
						target.writeByte(position);

						//System.out.println("wrote "+value+" "+position);

					} else {
						//System.out.println("wrote "+value);
						target.writePackedLong(value);
						//System.err.println("B write packed long "+value);
						//integers and rational only use normal long values, no position needed.
					}


				}
			}    

		}        

		return totalBytes;
	}
	

	
	public static <S extends MessageSchema<S>> boolean writeCapturedValuesToDataOutput(
			TrieParserReader reader, 
			DataOutputBlobWriter<S> target, 
			int[] indexPositions,
			Object[] validator) {
		
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		//NOTE: this method is used by the HTTP1xRouterStage class to write all the captured fields which is key to GreenLightning
		//      ensure that any changes here are matched by the methods consuming this DataOutput inside GreenLightnining.
		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		int limit = reader.capturedPos;
		int[] localCapturedValues = reader.capturedValues;

		boolean isValid = true;
		int fieldPosition = 0; //moves forward with each use.
		int totalBytes = 0;
		int i = 0;
		while (i < limit) {

			int type = localCapturedValues[i++];

			int writePosition = target.position();

			if (isCapturedByteData(type)) {

				int p = localCapturedValues[i++];
				int len = localCapturedValues[i++];
				int m = localCapturedValues[i++];   

				if (len>0) {
					totalBytes += len;
				}
				
				//logger.info("pipe:{} data pos {} idxPos {} captured text: {}",
	    		//        target.getPipe().id, writePosition, indexPositions[fieldPosition], Appendables.appendUTF8(new StringBuilder(), reader.capturedBlobArray, p, l, m));

	      
				//if those bytes were utf8 encoded then this matches the same as writeUTF8 without decode/encode                
				target.writeShort(len); //write the bytes count as a short first, then the UTF-8 encoded string
				if (len>0) {
					DataOutputBlobWriter.write(target,reader.sourceBacking,p,len,m);
				}

				if ((null!=validator) && (validator[fieldPosition] instanceof ByteSequenceValidator)) {
					isValid &= ((ByteSequenceValidator)validator[fieldPosition]).isValid(reader.sourceBacking,p&m,len,m);
				}
				
			} else {

				int sign = type;
				long value1 = localCapturedValues[i++];
				long value2 = localCapturedValues[i++]; 

				int meta = localCapturedValues[i++]; 
				boolean isDot = (meta<0);//if high bit is on this is a dot value
				byte base = (byte)((meta>>16)&0xFF);
				//int len  = meta&0xFFFF;

				long value = sign*((value1<<32)|value2);
				if (isDot) {
					if (base!=10) {
						throw new UnsupportedOperationException("Does support decimal point values with hex, please use base 10 decimal.");
					}
				} else {
					int position = 0;

					//Jump ahead to combine the dot part of the number if it is found.
					if (i+4<=limit //if there is following data
							&& (!isCapturedByteData(localCapturedValues[i])) //if next data is some kind of number	
							&& (localCapturedValues[i+3]<0)) { //if that next data point is the second half

						//decimal value                			
						//grab the dot value and roll it in.
						int dsign = localCapturedValues[i++];
						long dvalue1 = localCapturedValues[i++];
						long dvalue2 = localCapturedValues[i++];                    
						int dmeta = localCapturedValues[i++];

						byte dbase = (byte)((dmeta>>16)&0xFF);
						if (dbase!=10) {
							throw new UnsupportedOperationException("Does support decimal point values with hex, please use base 10 decimal.");
						}

						if (0 != position) {
							throw new UnsupportedOperationException("Expected left side of . to be a simple integer.");
						}

						int dlen  = dmeta&0xFFFF;

						long dvalue = dsign*((dvalue1<<32)|dvalue2);

						//shift the integer part up and add the decimal part
						value = (value*Decimal.longPow[dlen])+dvalue;

						//modify position to have the right number of points
						position = -dlen;  

						target.writePackedLong(value);

						DataOutputBlobWriter.setIntBackData(target, 
					               writePosition, 
					               indexPositions[fieldPosition++]);
						
						//write second part and it gets its own entry.
						writePosition = target.position();                		
						target.writeByte(position);

						if (null!=validator && validator[fieldPosition] instanceof DecimalValidator) {							
							isValid &= ((DecimalValidator)validator[fieldPosition]).isValid(value,(byte)position);
						}
					} else {
						//System.out.println("wrote "+value);
						target.writePackedLong(value);
						
						if (null!=validator && validator[fieldPosition] instanceof LongValidator) {							
							isValid &= ((LongValidator)validator[fieldPosition]).isValid(value);
						}
					}
				}
			}    

			
			DataOutputBlobWriter.setIntBackData(target, 
					               writePosition, 
					               indexPositions[fieldPosition++]);

		}        
		assert(fieldPosition==indexPositions.length);
		return isValid;
	}
	
	
	public static long capturedDecimalMField(TrieParserReader reader, int idx) {

		int pos = idx*4;
		assert(pos < reader.capturedValues.length) : 
			 "Either the idx argument ("+idx+") is too large or TrieParseReader was constructed ("+(reader.capturedValues.length/4)+") to hold too fiew fields";

		long sign = reader.capturedValues[pos++];
		assert(sign!=0);      	
		return (long) ((((long)reader.capturedValues[pos++])<<32) | (0xFFFFFFFFL&reader.capturedValues[pos++]))*sign; 
	}

	public static byte capturedDecimalEField(TrieParserReader reader, int idx) {
		int pos = (idx*4)+3;
		assert(pos < reader.capturedValues.length) : "Either the idx argument is too large or TrieParseReader was not constructed to hold this many fields";

		int meta = reader.capturedValues[pos];
		return (meta<0) ? (byte) -(meta & 0xFFFF) : (byte)0;
	}


	public static long capturedLongField(TrieParserReader reader, int idx) {

		int pos = idx*4;
		assert(pos < reader.capturedValues.length) : "Either the idx argument is too large or TrieParseReader was not constructed to hold this many fields";

		int sign = reader.capturedValues[pos++];
		assert(sign!=0);

		long value = (long) ((((long)reader.capturedValues[pos++])<<32) |
				             (0xFFFFFFFFL&reader.capturedValues[pos++]));

		return value*sign;
	}

	private static boolean isCapturedByteData(int type) {
		return 0==type;
	}

	public static int writeCapturedValuesToAppendable(TrieParserReader reader, Appendable target) throws IOException {
		int limit = reader.capturedPos;
		int[] localCapturedValues = reader.capturedValues;


		int totalBytes = 0;
		int i = 0;
		while (i < limit) {

			int type = localCapturedValues[i++];

			if (isCapturedByteData(type)) {

				int p = localCapturedValues[i++];
				int l = localCapturedValues[i++];
				int m = localCapturedValues[i++];   

				totalBytes += l;

				//if those bytes were utf8 encoded then this matches the same as writeUTF8 without decode/encode

				Appendables.appendValue(target, "[", l, "]");                
				Appendables.appendUTF8(target, reader.sourceBacking,p,l,m);

			} else {

				Appendables.appendValue(target, "[",type);
				Appendables.appendValue(target, ",",localCapturedValues[i++]);
				Appendables.appendValue(target, ",",localCapturedValues[i++]);
				Appendables.appendValue(target, ",",localCapturedValues[i++],"]");

			}            
		}
		return totalBytes;
	}




}
