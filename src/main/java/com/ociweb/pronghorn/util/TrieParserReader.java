package com.ociweb.pronghorn.util;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.util.math.Decimal;

public class TrieParserReader {

	private static final int LONGEST_LONG_HEX_DIGITS = 16;

	private static final int LONGEST_LONG_DIGITS = 19;

	private static final Logger logger = LoggerFactory.getLogger(TrieParserReader.class);

	private byte[] sourceBacking;
	public int    sourcePos;
	public  int   sourceLen;
	public int    sourceMask;


	private int[]  capturedValues;
	private int    capturedPos;
	private byte[] capturedBlobArray;


	private long    safeReturnValue = -1;
	private int     safeCapturedPos = -1;
	private int     saveCapturedLen = -1;
	private int     safeSourcePos = -1;

	private long result;
	private boolean normalExit;



	private final static int MAX_ALT_DEPTH = 256; //full recursion on alternate paths from a single point.
	private int altStackPos = 0;
	private int[] altStackA = new int[MAX_ALT_DEPTH];
	private int[] altStackB = new int[MAX_ALT_DEPTH];
	private int[] altStackC = new int[MAX_ALT_DEPTH];
	private int[] altStackD = new int[MAX_ALT_DEPTH];

	private short[] workingMultiStops = new short[MAX_ALT_DEPTH];
	private int[]   workingMultiContinue = new int[MAX_ALT_DEPTH];

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
		this(0, false);
	}

	public TrieParserReader(boolean alwaysCompletePayloads) {
		this(0, alwaysCompletePayloads);
	}

	public TrieParserReader(int maxCapturedFields) {
		this(maxCapturedFields,false);
	}

	public TrieParserReader(int maxCapturedFields, boolean alwaysCompletePayloads) {
		this.capturedValues = new int[maxCapturedFields*4];
		this.alwaysCompletePayloads = alwaysCompletePayloads;
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
		visit(that, 0, visitor, source, localSourcePos, sourceLength, sourceMask, -1);
	}

	private void visit(TrieParser that, final int i, ByteSquenceVisitor visitor, byte[] source, int localSourcePos, int sourceLength, int sourceMask, final long unfoundResult) {
		int run = 0;
		short[] data = that.data;

		visitor_initForQuery(this, that, source, localSourcePos, unfoundResult);
		pos = i+1; //used globally by other methods
		assert i<that.data.length: "the jumpindex: " + i + " exceeds the data length: "+that.data.length;
		type = that.data[i]; //used globally by other methods
		assert (type>-1) && (type<8) : "TYPE is not in range (0-7)";

		switch (type) {
		case TrieParser.TYPE_RUN:

			visitorRun(that, visitor, source, localSourcePos, sourceLength, sourceMask, unfoundResult, run, data);

			break;

		case TrieParser.TYPE_BRANCH_VALUE:
			
			visitorBranch(that, visitor, source, localSourcePos, sourceLength, sourceMask, unfoundResult, data);
			
			break;

		case TrieParser.TYPE_ALT_BRANCH:
		
			visitorAltBranch(that, i, visitor, source, localSourcePos, sourceLength, sourceMask, unfoundResult, data);
		  
		break;

		case TrieParser.TYPE_VALUE_NUMERIC:

			visitorNumeric(that, i, visitor, source, localSourcePos, sourceLength, sourceMask, unfoundResult, run);

			break;

		case TrieParser.TYPE_VALUE_BYTES:

			visitorValueBytes(that, i, visitor, source, localSourcePos, sourceLength, sourceMask, unfoundResult);

			break;

		case TrieParser.TYPE_SAFE_END:

			visitorSafeEnd(that, visitor, source, localSourcePos, sourceLength, sourceMask, unfoundResult);
			
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
			int sourceLength, int sourceMask, final long unfoundResult) {
		recordSafePointEnd(this, localSourcePos, pos, that);  
		pos += that.SIZE_OF_RESULT;
		if (sourceLength == this.runLength) {
			this.result = useSafePointNow(this);

			//add to result set
			visitor.addToResult(this.result);

			return;
		}   

		else{
			//recurse visit
			visit(that, this.pos, visitor, source, localSourcePos, sourceLength, sourceMask, unfoundResult);
		}
	}

	private void visitorValueBytes(TrieParser that, final int i, ByteSquenceVisitor visitor, byte[] source,
			int localSourcePos, int sourceLength, int sourceMask, final long unfoundResult) {
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
		visit(that, idx+byte_size, visitor, source, localSourcePos, sourceLength, sourceMask, unfoundResult);
	}

	private void visitorNumeric(TrieParser that, final int i, ByteSquenceVisitor visitor, byte[] source,
			int localSourcePos, int sourceLength, int sourceMask, final long unfoundResult, int run) {
		int idx;
		int temp_pos=0;
		idx = i + TrieParser.SIZE_OF_VALUE_NUMERIC;

		if (this.runLength<sourceLength && 
				(temp_pos = parseNumeric(that.ESCAPE_BYTE, this, source, localSourcePos, sourceLength, sourceMask, (int)that.data[pos++]))<0){
			System.out.println("returned from numeric, without moving further searching");
			return;
		}
		localSourcePos = temp_pos;

		//recurse into visit()
		visit(that, idx+run, visitor, source, localSourcePos, sourceLength, sourceMask, unfoundResult);
	}

	private void visitorAltBranch(TrieParser that, final int i, ByteSquenceVisitor visitor, byte[] source,
			int localSourcePos, int sourceLength, int sourceMask, final long unfoundResult, short[] data) {
		int localJump = i + TrieParser.SIZE_OF_ALT_BRANCH;
		//int farJump   = i + ((((int)that.data[i+2])<<15) | (0x7FFF&that.data[i+3])); 
		int jump = (((int)data[pos++])<<15) | (0x7FFF&data[pos++]); 

		visit(that, localJump, visitor, source, localSourcePos, sourceLength, sourceMask, unfoundResult);
		visit(that, localJump+jump, visitor, source, localSourcePos, sourceLength, sourceMask, unfoundResult);
	}

	private void visitorBranch(TrieParser that, ByteSquenceVisitor visitor, byte[] source, int localSourcePos,
			int sourceLength, int sourceMask, final long unfoundResult, short[] data) {
		if (this.runLength<sourceLength) {          
			//TrieMap data
			int p = pos;
			int jumpMask = TrieParser.computeJumpMask((short) source[sourceMask & localSourcePos], data[p++]);
			//System.out.println("jumpMask:"+jumpMask);
			pos = 0!=jumpMask ? computeJump(data, p, jumpMask) : 1+p;// u will get a specific jump location

			visit(that, pos, visitor, source, localSourcePos, sourceLength, sourceMask, unfoundResult);//only that jump
		}
		else{
			return;
		}
	}

	private void visitorRun(TrieParser that, ByteSquenceVisitor visitor, byte[] source, int localSourcePos,
			int sourceLength, int sourceMask, final long unfoundResult, int run, short[] data) {
		int idx;
		final int sourceMask1 = sourceMask;
		short[] localData = data;
		byte caseMask = that.caseRuleMask;
		int r1 = run;
		int t1 = pos;
		int t2 = localSourcePos;
		while ((--r1 >= 0) && ((caseMask&localData[t1++]) == (caseMask&source[sourceMask1 & t2++])) ) {
		}
		pos = t1;
		localSourcePos = t2;

		int r = r1;
		if (r >= 0) {
			return;	
		} else {        
			run = that.data[pos];
			idx = pos + TrieParser.SIZE_OF_RUN-1;
			visit(that, idx+run, visitor, source, localSourcePos+run, sourceLength, sourceMask, unfoundResult);
		}
	}

	private static void visitor_initForQuery(TrieParserReader reader, TrieParser trie, byte[] source, int sourcePos, long unfoundResult) {
		reader.capturedPos = 0;
		reader.capturedBlobArray = source;
		//working vars
		reader.pos = 0;
		reader.runLength = 0;
		reader.localSourcePos =sourcePos;
		reader.result = unfoundResult;
		reader.normalExit = true;
		reader.altStackPos = 0; 

		assert(trie.getLimit()>0) : "SequentialTrieParser must be setup up with data before use.";

		reader.type = trie.data[reader.pos++];
	}


	public static void parseSetup(TrieParserReader that, byte[] source, int offset, int length, int mask) {
		assert(length<=source.length) : "length is "+length+" but the array is only "+source.length;
		that.sourceBacking = source;
		//        if (offset<that.sourcePos) {
		//        	new Exception("warning moved backwards").printStackTrace();;
		//        }
		that.sourcePos     = offset;
		that.sourceLen     = length;
		that.sourceMask    = mask;        
	}

	public static void parseSetup(TrieParserReader that, byte[] source, int mask) {
		that.sourceBacking = source;
		that.sourceMask    = mask;        
	}


	public static void parseSetupGrow(TrieParserReader that, int additionalLength) {
		that.sourceLen += additionalLength;
		assert(that.sourceLen<=that.sourceMask) : "length is out of bounds";
	}

	/**
	 * Save position and return the current length
	 * @param that
	 * @param target
	 * @param offset
	 * @return length of remaining position.
	 */
	public static int savePositionMemo(TrieParserReader that, int[] target, int offset) {

		target[offset] = that.sourcePos;
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
	public static int debugAsUTF8(TrieParserReader that, Appendable target, int maxLen, boolean mayHaveLeading) {
		int pos = that.sourcePos;
		try {
			if (mayHaveLeading && ((that.sourceBacking[pos & that.sourceMask]<32) || (that.sourceBacking[(1+pos) & that.sourceMask]<32))) {
				//we have a leading length
				target.append("[");
				Appendables.appendValue(target, that.sourceBacking[that.sourceMask & pos++]);
				target.append(",");
				Appendables.appendValue(target, that.sourceBacking[that.sourceMask & pos++]);
				target.append("]");
				that.sourceLen-=2;
			}

			Appendable a = Appendables.appendUTF8(target, that.sourceBacking, pos, Math.min(maxLen, that.sourceLen), that.sourceMask);
			if (maxLen<that.sourceLen) {
				a.append("...");
			}
		} catch (Exception e) {
			//this was not UTF8 so dump the chars
			Appendables.appendArray(target, '[', that.sourceBacking, pos, that.sourceMask, ']', Math.min(maxLen, that.sourceLen));
		}
		return pos;
	}

	public static boolean parseHasContent(TrieParserReader reader) {
		return reader.sourceLen>0;
	}

	public static int parseHasContentLength(TrieParserReader reader) {
		return reader.sourceLen;
	}


	public static long parseNext(TrieParserReader reader, TrieParser trie) {

		final int originalPos = reader.sourcePos;
		final int originalLen = reader.sourceLen;   
		final int notFound = -1;

		long result =  query(reader, trie, reader.sourceBacking, originalPos, originalLen, reader.sourceMask, notFound);

		//Hack for now
		if (reader.sourceLen < 0) {
			//logger.info("warning trieReader is still walking past end");
			//TODO: URGENT FIX requred, this is an error in the trieReader the pattern "%b: %b\r\n" goes past the end and must be invalidated
			result = notFound;//invalidate any selection
		}
		//end of hack


		if (result!=notFound) {
			return result;
		} else {
			//not found so roll the pos and len back for another try later
			reader.sourcePos = originalPos;
			reader.sourceLen = originalLen;
			return result;
		}

	}

	private static void reportError(TrieParserReader reader, int debugPos, int debugLen) {
		String input = "";

		input = debugContent(reader, debugPos, debugLen);

		System.out.println("pos:"+debugPos+" len:"+debugLen+" unable to parse:\n'"+input+"'");
	}

	private static String debugContent(TrieParserReader reader, int debugPos, int debugLen) {
		return Appendables.appendUTF8(new StringBuilder(), 
				reader.sourceBacking, 
				debugPos, 
				Math.min(500,(int)debugLen), 
				reader.sourceMask).toString();
	}

	public static void logNextTextToParse(TrieParserReader reader) {
		StringBuilder builder = new StringBuilder();
		int c = Math.min(500, (int)reader.sourceLen);
		int p = reader.sourcePos;
		logger.warn("parse pos:{} len:{}",p,reader.sourceLen);
		while (--c>=0) {
			builder.append((char)reader.sourceBacking[reader.sourceMask & p++]);
		}
		String toParse = builder.toString();
		logger.warn("to parse next:\n{}",toParse);
	}

	public static int parseSkip(TrieParserReader reader, int count) {

		int len = Math.min(count, reader.sourceLen);
		reader.sourcePos += len;
		reader.sourceLen -= len;

		return len;
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
		int meta = Pipe.takeRingByteMetaData(input);
		int length    = Pipe.takeRingByteLen(input);
		return query(trieReader, trie, Pipe.byteBackingArray(meta, input), Pipe.bytePosition(meta, input, length), length, Pipe.blobMask(input), unfoundResult );  
	}


	public static long query(TrieParserReader reader, TrieParser trie, 
			byte[] source, int localSourcePos, int sourceLength, int sourceMask) {
		return query(reader,trie,source,localSourcePos, sourceLength, sourceMask, -1);
	}


	private int pos;
	private int runLength;
	private int type;
	private int localSourcePos;

	public static long query(TrieParserReader reader, TrieParser trie, 
			byte[] source, int sourcePos, long sourceLength, int sourceMask, final long unfoundResult) {

		if (trie.getLimit()>0) {
		} else {
			return unfoundResult;
		}

		initForQuery(reader, trie, source, sourcePos, unfoundResult);        
		processEachType(reader, trie, source, sourceLength, sourceMask, unfoundResult);

		if (reader.normalExit) {
			return exitUponParse(reader, trie);        	       	 
		} else {
			return exitWithNoParse(reader, trie);
		}        

	}

	private static long exitUponParse(TrieParserReader reader, TrieParser trie) {
		reader.sourceLen -= (reader.localSourcePos-reader.sourcePos);
		reader.sourcePos = reader.localSourcePos;        	        	
		return TrieParser.readEndValue(trie.data,reader.pos, trie.SIZE_OF_RESULT);
	}

	private static long exitWithNoParse(TrieParserReader reader, TrieParser trie) {
		//unable to parse this case
		//determine if we have an error or need more data        	
		//assert(reader.sourceLen <= trie.longestKnown()) : "Check the parse tree, this text was not parsable. "+debugContent(reader, reader.sourcePos, reader.sourceLen);
		boolean debug = false;
		if (debug && (reader.sourceLen > trie.longestKnown())) {
			logger.info("WARNING: input data can not be parsed and the pipeline has stopped at this point. {}",debugContent(reader, reader.sourcePos, reader.sourceLen) ,new Exception());
		} //else we just need more data.

		return reader.result;
	}

	private static void processEachType(TrieParserReader reader, TrieParser trie, byte[] source, long sourceLength,
			int sourceMask, final long unfoundResult) {

		boolean hasSafePoint = false;
		int t = 0;

		while ((t=reader.type) != TrieParser.TYPE_END && reader.normalExit) {  

			if (t == TrieParser.TYPE_RUN) {                
				parseRun(reader, trie, source, sourceLength, sourceMask, unfoundResult, hasSafePoint);
			} else {
				hasSafePoint = lessCommonActions(reader, trie, source, sourceLength, sourceMask, unfoundResult,	hasSafePoint, t);
				reader.type = trie.data[reader.pos++]; 
			}

		}
	}

	private static void parseRun(TrieParserReader reader, TrieParser trie, byte[] source, long sourceLength,
			int sourceMask, final long unfoundResult, boolean hasSafePoint) {
		//run
		final int run = trie.data[reader.pos++];    

		//we will not have the room to do a match.
		if (reader.runLength+run > sourceLength && !hasSafePoint && 0==reader.altStackPos) {
			reader.normalExit=false;
			reader.result = unfoundResult;
			reader.runLength += run;   
		} else if (!(trie.skipDeepChecks && !hasSafePoint && 0==reader.altStackPos)) {
			scanForRun(reader, trie, source, sourceMask, unfoundResult, hasSafePoint, run);
		} else {
			reader.pos += run;
			reader.localSourcePos += run; 
			reader.runLength += run;
			reader.type = trie.data[reader.pos++];
		}
	}

	private static void scanForRun(TrieParserReader reader, TrieParser trie, byte[] source, int sourceMask,
			final long unfoundResult, boolean hasSafePoint, final int run) {
		//scan returns -1 for a perfect match
		int r = scanForMismatch(reader, source, sourceMask, trie, run);
		if (r >= 0) {
			if (!hasSafePoint) {                       	
				if (reader.altStackPos > 0) {                                
					loadupNextChoiceFromStack(reader, trie.data);                           
				} else {
					reader.normalExit=false;
					reader.result = unfoundResult;                   
					reader.runLength += run;
					reader.type = trie.data[reader.pos++];
				}
			} else {
				reader.normalExit=false;
				reader.result = useSafePoint(reader);                        	
				reader.runLength += run;
				reader.type = trie.data[reader.pos++];
			}
		} else {        
			reader.runLength += run;
			reader.type = trie.data[reader.pos++];
		}
	}

	private static boolean lessCommonActions(TrieParserReader reader, TrieParser trie, byte[] source, long sourceLength,
			int sourceMask, final long unfoundResult, boolean hasSafePoint, int t) {
		if (t==TrieParser.TYPE_BRANCH_VALUE) {   
			if (reader.runLength<sourceLength) {              
				short[] data = trie.data;
				int p = reader.pos;

				int jumpMask = TrieParser.computeJumpMask((short) source[sourceMask & reader.localSourcePos], data[p++]);

				reader.pos = 0!=jumpMask ? computeJump(data, p, jumpMask) : 1+p;
			} else {
				reader.normalExit=false;
				reader.result = unfoundResult;

			}
		} else if (t == TrieParser.TYPE_ALT_BRANCH) {
			processAltBranch(reader, trie.data);
		} else {
			hasSafePoint = lessCommonActions2(reader, trie, source, sourceLength, sourceMask, unfoundResult, hasSafePoint, t);
		}
		return hasSafePoint;
	}

	private static int computeJump(short[] data, int p, int jumpMask) {
		return 1+(jumpMask&((((int)data[p++])<<15) | (0x7FFF&data[p])))+p;
	}

	private static boolean lessCommonActions2(TrieParserReader reader, TrieParser trie, byte[] source,
			final long sourceLength, int sourceMask, final long unfoundResult, final boolean hasSafePoint, int t) {

		if (t == TrieParser.TYPE_VALUE_BYTES) {            	
			parseBytesAction(reader, trie, source, sourceLength, sourceMask, unfoundResult);
			return hasSafePoint;
		} else {
			return lessCommonActions3(reader, trie, source, sourceLength, sourceMask, unfoundResult, hasSafePoint, t);
		}
	}

	private static void parseBytesAction(final TrieParserReader reader, final TrieParser trie, final byte[] source,
			final long sourceLength, final int sourceMask, final long unfoundResult) {

		if ((reader.runLength < sourceLength) && parseBytes(reader, trie, source, sourceLength, sourceMask)) {
		} else {
			reader.normalExit = false;
			reader.result = unfoundResult;
		}
	}

	private static boolean lessCommonActions3(TrieParserReader reader, TrieParser trie, byte[] source,
			long sourceLength, int sourceMask, final long unfoundResult, boolean hasSafePoint, int t) {
		if (t == TrieParser.TYPE_VALUE_NUMERIC) {       
			if (reader.runLength<sourceLength) {
				if ((reader.localSourcePos = parseNumeric(trie.ESCAPE_BYTE, reader,source,reader.localSourcePos, sourceLength-reader.runLength, sourceMask, (int)trie.data[reader.pos++]))<0) {			            	
					reader.normalExit=false;
					reader.result = unfoundResult;

				}    
			} else {
				reader.normalExit=false;
				reader.result = unfoundResult;

			}
		}  else if (t == TrieParser.TYPE_SAFE_END) {                    

			recordSafePointEnd(reader, reader.localSourcePos, reader.pos, trie);  
			hasSafePoint = true;
			reader.pos += trie.SIZE_OF_RESULT;
			if (sourceLength == reader.runLength) {
				reader.normalExit=false;
				reader.result = useSafePointNow(reader);

			}                                             
		} else  {       
			reportError(reader, trie);
		}
		return hasSafePoint;
	}

	private static void reportError(TrieParserReader reader, TrieParser trie) {
		logger.error(trie.toString());
		throw new UnsupportedOperationException("Bad jump length now at position "+(reader.pos-1)+" type found "+reader.type);
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

			if (reader.altStackPos>0) {
				short[] localData = trie.data;

				int i = reader.altStackPos; 
				int[] localAltStackD = reader.altStackD;
				int[] localAltStackC = reader.altStackC;
				int[] localAltStackB = reader.altStackB;
				int[] localAltStackA = reader.altStackA;


				while (--i>=0) {
					int cTemp = localAltStackC[i];
					if (localData[cTemp] == TrieParser.TYPE_VALUE_BYTES) {

						if (localCaputuredPos != localAltStackB[i]) {//part of the same path.
							break;
						}
						if (localSourcePos != localAltStackA[i]) {//part of the same path.
							break;
						}
						if (localRunLength != localAltStackD[i]){
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

			}	

			if (stopCount>1) {				
				assert(localWorkingMultiStops.length>0);
				int x = localSourcePos;
				int lim = maxCapture<=sourceMask ? (int)maxCapture : sourceMask+1;	        	

				if (stopCount==2) {
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
			} else {
				return -1 != (reader.localSourcePos = parseBytes(reader,source,reader.localSourcePos, maxCapture, sourceMask, stopValue));
			}
		} else {
			reader.localSourcePos = -1;
			return false;
		}
	}

	private static boolean assignParseBytesResults(final TrieParserReader reader, final int sourceMask,
			final int localSourcePos, int x) {
		int len = (x-localSourcePos)-1;
		reader.runLength += (len);

		reader.capturedPos = extractedBytesRange(reader.capturedBlobArray, reader.capturedValues, reader.capturedPos, localSourcePos, len, sourceMask);  
		reader.localSourcePos = x;
		return true;
	}

	private static boolean assignParseBytesResults(TrieParserReader reader, int sourceMask, final int sourcePos, int x, int stopIdx) {

		//this is for the case where we match up to the very end of the string		
		if (reader.alwaysCompletePayloads && -1 == stopIdx) {
			int j = reader.workingMultiStops.length;

			System.out.println("multi stops "+j);

			while (--j>=0) {
				if (reader.workingMultiStops[j]==0) {
					stopIdx = j;
				}
			}
		}

		if (-1==stopIdx) {//not found!
			reader.localSourcePos =-1;

			System.err.println("not found B");

			return false;
		} else {
			int len = (x-sourcePos)-1;
			reader.runLength += (len);

			reader.capturedPos = extractedBytesRange(reader.capturedBlobArray ,reader.capturedValues, reader.capturedPos, sourcePos, len, sourceMask);  
			reader.localSourcePos = x;
			reader.pos = reader.workingMultiContinue[stopIdx];
			return true;
		}
	}

	private static void initForQuery(TrieParserReader reader, TrieParser trie, byte[] source, int sourcePos, long unfoundResult) {
		reader.capturedPos = 0;
		reader.capturedBlobArray = source;
		//working vars
		reader.pos = 0;
		reader.runLength = 0;
		reader.localSourcePos =sourcePos;
		reader.result = unfoundResult;
		reader.normalExit = true;
		reader.altStackPos = 0; 

		assert(trie.getLimit()>0) : "SequentialTrieParser must be setup up with data before use.";

		reader.type = trie.data[reader.pos++];
	}

	private static void processAltBranch(TrieParserReader reader, short[] localData) {
		assert(localData[reader.pos]>=0): "bad value "+localData[reader.pos];
		assert(localData[reader.pos+1]>=0): "bad value "+localData[reader.pos+1];

		//the extracted (byte or number) is ALWAYS local so push LOCAL position on stack and take the JUMP        	

		int pos = reader.pos;
		int offset = reader.localSourcePos;
		int jump = (((int)localData[reader.pos++])<<15) | (0x7FFF&localData[reader.pos++]); 
		int runLength = reader.runLength;
		assert(jump>0) : "Jump must be postitive but found "+jump;

		int aLocal = pos+ TrieParser.BRANCH_JUMP_SIZE;
		int bJump = pos+jump+ TrieParser.BRANCH_JUMP_SIZE;

		int localType = localData[aLocal];

		assert(isAltType(localType) || TrieParser.TYPE_BRANCH_VALUE == localType) : "unknown value of: "+localType;  // local can only be one of the capture types or a branch leaning to those exclusively.


		if (isAltType(localType)) {  

			//this is the normal expected case
			//push local on stack so we can try the captures if the literal does not work out. (NOTE: assumes all literals are found as jumps and never local)
			pushAlt(reader, aLocal, offset, runLength);

			//take the jump   		    
			reader.pos = bJump;

		} else {

			logger.warn("TrieParserRead has had to process Alt out of order because it was constructed in the wrong order");
			//push local on stack so we can try the captures if the literal does not work out. (NOTE: assumes all literals are found as jumps and never local)
			pushAlt(reader, bJump, offset, runLength);

			//take the jump   		    
			reader.pos = aLocal;
		}

	}

	private static boolean isAltType(int localType) {
		return TrieParser.TYPE_VALUE_BYTES == localType || 
				TrieParser.TYPE_VALUE_NUMERIC  == localType ||
				TrieParser.TYPE_ALT_BRANCH == localType;
	}

	private static long useSafePointNow(TrieParserReader reader) {
		//hard stop passed in forces us to use the safe point
		reader.sourceLen -= (reader.localSourcePos-reader.sourcePos);
		reader.sourcePos = reader.localSourcePos;
		return reader.safeReturnValue;
	}

	private static void loadupNextChoiceFromStack(TrieParserReader reader, short[] localData) {
		//try other path
		//reset all the values to the other path and continue from the top

		reader.localSourcePos     = reader.altStackA[--reader.altStackPos];
		reader.capturedPos = reader.altStackB[reader.altStackPos];
		reader.pos         = reader.altStackC[reader.altStackPos];
		reader.runLength = reader.altStackD[reader.altStackPos];                                

		reader.type = localData[reader.pos++];
	}

	private static int scanForMismatch(TrieParserReader reader, byte[] source, final int sourceMask, TrieParser trie, int run) {


		short[] localData = trie.data;
		byte caseMask = trie.caseRuleMask;
		int r = run;
		int t1 = reader.pos;
		int t2 = reader.localSourcePos;
		while ((--r >= 0) && ((caseMask&localData[t1++]) == (caseMask&source[sourceMask & t2++])) ) {
		}
		reader.pos = t1;
		reader.localSourcePos = t2;
		return r;
	}

	private static long useSafePoint(TrieParserReader reader) {
		reader.localSourcePos  = reader.safeSourcePos;
		reader.capturedPos = reader.safeCapturedPos;
		reader.sourceLen = reader.saveCapturedLen;
		reader.sourcePos =reader.localSourcePos;

		return reader.safeReturnValue;
	}

	static void pushAlt(TrieParserReader reader, int pos, int offset, int runLength) {

		reader.altStackA[reader.altStackPos] = offset;
		reader.altStackB[reader.altStackPos] = reader.capturedPos;
		reader.altStackC[reader.altStackPos] = pos;        
		reader.altStackD[reader.altStackPos++] = runLength;
	}

	private static void recordSafePointEnd(TrieParserReader reader, int localSourcePos, int pos, TrieParser trie) {

		reader.safeReturnValue = TrieParser.readEndValue(trie.data, pos, trie.SIZE_OF_RESULT);
		reader.safeCapturedPos = reader.capturedPos;
		reader.saveCapturedLen = reader.sourceLen;

		reader.safeSourcePos = localSourcePos;        


		//if the following does not match we will return this safe value.
		//we do not yet have enough info to decide if this is the end or not.
	}

	private static int parseBytes(TrieParserReader reader, final byte[] source, final int sourcePos, long remainingLen, final int sourceMask, final short stopValue) {              

		int x = sourcePos;
		int lim = remainingLen<=sourceMask ? (int)remainingLen : sourceMask+1;
		boolean noStop = true;
		do {
		} while ( (noStop=(stopValue!=source[sourceMask & x++])) && (--lim > 0));         

		if (noStop && 0!=stopValue) { //a zero stop value is a rule to caputure evertything up to the end of the data.
			return -1;//not found!
		}
		return parseBytesFound(reader, sourcePos, sourceMask, x);
	}

	private static int parseBytesFound(TrieParserReader reader, final int sourcePos, final int sourceMask, int x) {
		int len = (x-sourcePos)-1;

		reader.runLength += (len);
		reader.capturedPos = extractedBytesRange(reader.capturedBlobArray ,reader.capturedValues, reader.capturedPos, sourcePos, len, sourceMask);                
		return x;
	}

	private static int indexOfMatchInArray(short value, short[] data, int i) {
		//System.err.println("searhing run of "+count); checked and found it 1
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

	private static int parseNumeric(final byte escapeByte, TrieParserReader reader, byte[] source, int sourcePos, long sourceLength, int sourceMask, int numType) {

		int basePos = sourcePos;

		byte sign = 1;
		long intValue = 0;
		byte intLength = 0;
		byte base=10;
		int  dot=0;//only set to one for NUMERIC_FLAG_DECIMAL        

		final short c1 = source[sourceMask & sourcePos];

		if (escapeByte == c1) {
			sourcePos++;
			byte numericType = source[sourceMask & sourcePos];
			int typeMask = TrieParser.buildNumberBits(numericType);
			sourcePos++;

			if ((typeMask&numType)==typeMask) {
				return sourcePos;
			} else {
				return -1;
			}
		}


		if (0!= (TrieParser.NUMERIC_FLAG_DECIMAL&numType)) {
			dot=1;
			if ('.'!=c1) {
				publish(reader, 1, 0, 1, 10, dot);
				//do not parse numeric
				return sourcePos;
			} else {
				sourcePos++;
			}

		} else if (0!= (TrieParser.NUMERIC_FLAG_RATIONAL&numType)) {
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
			if (c1=='-') { //NOTE: check ASCII table there may be a fater way to do this.
				sign = -1;
				sourcePos++;
			}
			if (c1=='+') {
				sourcePos++;
			}
		}



		boolean hasNo0xPrefix = ('0'!=source[sourceMask & sourcePos+1]) || ('x'!=source[sourceMask & sourcePos+2]);
		if (hasNo0xPrefix && 0==(TrieParser.NUMERIC_FLAG_HEX&numType) ) {    
			//just to keep it from spinning on values that are way out of bounds
			sourceLength = Math.min(LONGEST_LONG_DIGITS+1, sourceLength); //never scan over 32

			base = 10;
			short c = 0;
			do {
				c = source[sourceMask & sourcePos++];        

				if (intLength<sourceLength) {

					if ((c>='0') && (c<='9') ) {
						intValue = (intValue * 10)+(c-'0');
						intLength++;
						continue;
					} else {
						break;//next char is not valid.
					}
				} else {
					if (reader.alwaysCompletePayloads) {
						break;
					} else {
						return -1; //we are waiting for more digits in the feed. 
					}
				}

			}  while (true);

		} else {
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
					if (reader.alwaysCompletePayloads) {
						//do not reset the length;
					} else {
						//we are waiting for more digits in the feed. 
						// intLength>=sourceLength
						intLength=0;
					}
					break;
				}

			}  while (true);

		}


		if (intLength==0) {
			return -1;
		}
		publish(reader, sign, intValue, intLength, base, dot);

		return sourcePos-1;
	}

	private static void publish(TrieParserReader reader, int sign, long numericValue, int intLength, int base, int isDot) {
		assert(0!=sign);

		reader.capturedValues[reader.capturedPos++] = sign;
		reader.capturedValues[reader.capturedPos++] = (int) (numericValue >> 32);
		reader.capturedValues[reader.capturedPos++] = (int) (0xFFFFFFFF &numericValue);

		assert(base<=64 && base>=2);
		assert(isDot==1 || isDot==0);

		reader.capturedValues[reader.capturedPos++] = (isDot<<31) | (base<<16) | (0xFFFF & intLength) ; //Base: 10 or 16, IntLength:  

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
//<<<<<<< HEAD
		}        
	}

	/*public static void writeCapturedUTF8(TrieParserReader reader, int idx, DataOutputBlobWriter<?> target) {
		int pos = idx*4;

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int p = reader.capturedValues[pos++];
		int l = reader.capturedValues[pos++];
		int m = reader.capturedValues[pos++];

		//this data is already encoded as UTF8 so we do a direct copy
		target.writeShort(l);
		DataOutputBlobWriter.write(target, reader.capturedBlobArray, p, l, m);

	}

	public static void parseSetup(TrieParserReader trieReader, int loc, Pipe<?> input) {

		parseSetup(trieReader, PipeReader.readBytesBackingArray(input, loc), 
				PipeReader.readBytesPosition(input, loc), 
				PipeReader.readBytesLength(input, loc), 
				PipeReader.readBytesMask(input, loc));
	}


	public static void parseSetup(TrieParserReader trieReader, Pipe<?> input) {
		int meta = Pipe.takeRingByteMetaData(input);
		int length    = Pipe.takeRingByteLen(input);
		parseSetup(trieReader, Pipe.byteBackingArray(meta, input), Pipe.bytePosition(meta, input, length), length, Pipe.blobMask(input));
	}


	public static int capturedFieldCount(TrieParserReader reader) {
		return reader.capturedPos>>2;
	}

	public static void capturedFieldInts(TrieParserReader reader, int idx, int[] targetArray, int targetPos) {

		int pos = idx*4;

		int type = reader.capturedValues[pos++];
		assert(type!=0);
		targetArray[targetPos++] = type;
		targetArray[targetPos++] = reader.capturedValues[pos++];
		targetArray[targetPos++] = reader.capturedValues[pos++];
		targetArray[targetPos++] = reader.capturedValues[pos++];

	}

	public static int capturedFieldBytes(TrieParserReader reader, int idx, byte[] target, int targetPos, int targetMask) {

		int pos = idx*4;

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int p = reader.capturedValues[pos++];
		int l = reader.capturedValues[pos++];
		int m = reader.capturedValues[pos++];

		Pipe.copyBytesFromToRing(reader.capturedBlobArray, p, m, target, targetPos, targetMask, l);

		return l;
	}

	public static int capturedFieldByte(TrieParserReader reader, int idx, int offset) {

		int pos = idx*4;

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int p = reader.capturedValues[pos++];
		int l = reader.capturedValues[pos++];
		int m = reader.capturedValues[pos++];

		if (offset<l) {
			return 0xFF & reader.capturedBlobArray[m & (p+offset)];            
		} else {
			return -1;
		}
	}


	public static int capturedFieldBytes(TrieParserReader reader, int idx, ByteConsumer target) {
		assert(null!=reader);
		assert(null!=target);

		int pos = idx*4;

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int bpos = reader.capturedValues[pos++];
		int blen = reader.capturedValues[pos++];
		int bmsk = reader.capturedValues[pos++];

		target.consume(reader.capturedBlobArray, bpos, blen, bmsk);
		return blen;

	}

	public static long capturedFieldQuery(TrieParserReader reader, int idx, TrieParser trie) {
		//two is the default for the stop bytes.
		return capturedFieldQuery(reader,idx,2,trie);
	}

	//parse the capture text as a query against yet another trie
	public static <A extends Appendable> long capturedFieldQuery(TrieParserReader reader, int idx, int stopBytesCount, TrieParser trie) {

		int pos = idx*4;

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int bpos = reader.capturedValues[pos++];
		int blen = reader.capturedValues[pos++];
		int bmsk = reader.capturedValues[pos++];

		//we add 2 to the length to pick up the stop chars, this ensure we have enough text to match
		return query(reader, trie, reader.capturedBlobArray, bpos, blen+stopBytesCount, bmsk, -1);

	}

	public static void capturedFieldSetValue(TrieParserReader reader, int idx, TrieParser trie, long value) {

		int pos = idx*4;

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int bpos = reader.capturedValues[pos++];
		int blen = reader.capturedValues[pos++];
		int bmsk = reader.capturedValues[pos++];

		trie.setValue(reader.capturedBlobArray, bpos, blen, bmsk, value);

	}

	public static <A extends Appendable> A capturedFieldBytesAsUTF8(TrieParserReader reader, int idx, A target) {

		int pos = idx*4;

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int bpos = reader.capturedValues[pos++];
		int blen = reader.capturedValues[pos++];
		int bmsk = reader.capturedValues[pos++];

		return Appendables.appendUTF8(target, reader.capturedBlobArray, bpos, blen, bmsk);

	}

	public static <A extends Appendable> A capturedFieldBytesAsUTF8Debug(TrieParserReader reader, int idx, A target) {

		int pos = idx*4;

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int bpos = reader.capturedValues[pos++];
		int blen = reader.capturedValues[pos++];
		int bmsk = reader.capturedValues[pos++];

		return Appendables.appendUTF8(target, reader.capturedBlobArray, bpos-10, blen+20, bmsk);

	}

	public static int writeCapturedUTF8ToPipe(TrieParserReader reader, Pipe<?> target, int idx, int loc) {
		int pos = idx*4;

		int type = reader.capturedValues[pos++];
		assert(type==0);
		int bpos = reader.capturedValues[pos++];
		int blen = reader.capturedValues[pos++];
		PipeWriter.writeBytes(target, loc, reader.capturedBlobArray, bpos, blen, reader.capturedValues[pos++]);

		return blen;

	}


	public static <S extends MessageSchema<S>> int writeCapturedValuesToDataOutput(TrieParserReader reader, DataOutputBlobWriter<S> target, boolean writeIndex) throws IOException {
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
				DataOutputBlobWriter.write(target,reader.capturedBlobArray,p,l,m);

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
						//System.err.println("A write packed long "+value);
						if (writeIndex && !DataOutputBlobWriter.tryWriteIntBackData(target, writePosition)) {
							throw new IOException("Pipe var field length is too short for "+DataOutputBlobWriter.class.getSimpleName()+" change config for "+target.getPipe());
						}                         
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

			if (writeIndex && !DataOutputBlobWriter.tryWriteIntBackData(target, writePosition)) {
				throw new IOException("Pipe var field length is too short for "+DataOutputBlobWriter.class.getSimpleName()+" change config for pipe varLength is "+target.getPipe().maxVarLen);
			}


		}        

		return totalBytes;
	}

	public static long capturedDecimalMField(TrieParserReader reader, int idx) {

		int pos = idx*4;

		long sign = reader.capturedValues[pos++];
		assert(sign!=0);      	
		return (long) ((((long)reader.capturedValues[pos++])<<32) | (0xFFFFFFFFL&reader.capturedValues[pos++]))*sign; 
	}

	public static byte capturedDecimalEField(TrieParserReader reader, int idx) {

		int meta = reader.capturedValues[(idx*4)+3];
		return (meta<0) ? (byte) -(meta & 0xFFFF) : (byte)0;
	}



	public static long capturedLongField(TrieParserReader reader, int idx) {

		int pos = idx*4;

		int sign = reader.capturedValues[pos++];
		assert(sign!=0);

		long value = reader.capturedValues[pos++];
		value = (value<<32) | (0xFFFFFFFF&reader.capturedValues[pos++]);

		int meta = reader.capturedValues[pos];
		///  byte dbase = (byte)((meta>>16)&0xFF);
=======
		 }        
    }
*/    
    public static void writeCapturedUTF8(TrieParserReader reader, int idx, DataOutputBlobWriter<?> target) {
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int p = reader.capturedValues[pos++];
        int l = reader.capturedValues[pos++];
        int m = reader.capturedValues[pos++];
        
        //this data is already encoded as UTF8 so we do a direct copy
        target.writeShort(l);
        DataOutputBlobWriter.write(target, reader.capturedBlobArray, p, l, m);
    
   }
    
    public static void parseSetup(TrieParserReader trieReader, int loc, Pipe<?> input) {
    	
        parseSetup(trieReader, PipeReader.readBytesBackingArray(input, loc), 
        		               PipeReader.readBytesPosition(input, loc), 
        		               PipeReader.readBytesLength(input, loc), 
        		               PipeReader.readBytesMask(input, loc));
    }
    

    public static void parseSetup(TrieParserReader trieReader, Pipe<?> input) {
        int meta = Pipe.takeRingByteMetaData(input);
        int length    = Pipe.takeRingByteLen(input);
        parseSetup(trieReader, Pipe.byteBackingArray(meta, input), Pipe.bytePosition(meta, input, length), length, Pipe.blobMask(input));
    }
    
    
    public static int capturedFieldCount(TrieParserReader reader) {
        return reader.capturedPos>>2;
    }
    
    public static void capturedFieldInts(TrieParserReader reader, int idx, int[] targetArray, int targetPos) {
        
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type!=0);
        targetArray[targetPos++] = type;
        targetArray[targetPos++] = reader.capturedValues[pos++];
        targetArray[targetPos++] = reader.capturedValues[pos++];
        targetArray[targetPos++] = reader.capturedValues[pos++];
        
    }
    
    public static int capturedFieldBytes(TrieParserReader reader, int idx, byte[] target, int targetPos, int targetMask) {
        
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int p = reader.capturedValues[pos++];
        int l = reader.capturedValues[pos++];
        int m = reader.capturedValues[pos++];

        Pipe.copyBytesFromToRing(reader.capturedBlobArray, p, m, target, targetPos, targetMask, l);
        
        return l;
    }
    
    public static int capturedFieldByte(TrieParserReader reader, int idx, int offset) {
        
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int p = reader.capturedValues[pos++];
        int l = reader.capturedValues[pos++];
        int m = reader.capturedValues[pos++];

        if (offset<l) {
            return 0xFF & reader.capturedBlobArray[m & (p+offset)];            
        } else {
            return -1;
        }
    }


    public static int capturedFieldBytes(TrieParserReader reader, int idx, ByteConsumer target) {
        assert(null!=reader);
        assert(null!=target);
        
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int bpos = reader.capturedValues[pos++];
        int blen = reader.capturedValues[pos++];
        int bmsk = reader.capturedValues[pos++];
        
        target.consume(reader.capturedBlobArray, bpos, blen, bmsk);
        return blen;

    }
    
    public static long capturedFieldQuery(TrieParserReader reader, int idx, TrieParser trie) {
    	  //two is the default for the stop bytes.
    	  return capturedFieldQuery(reader,idx,2,trie);
    }
    
    //parse the capture text as a query against yet another trie
    public static <A extends Appendable> long capturedFieldQuery(TrieParserReader reader, int idx, int stopBytesCount, TrieParser trie) {
        
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int bpos = reader.capturedValues[pos++];
        int blen = reader.capturedValues[pos++];
        int bmsk = reader.capturedValues[pos++];
        
        //we add 2 to the length to pick up the stop chars, this ensure we have enough text to match
        return query(reader, trie, reader.capturedBlobArray, bpos, blen+stopBytesCount, bmsk, -1);

    }
    
    public static void capturedFieldSetValue(TrieParserReader reader, int idx, TrieParser trie, long value) {
        
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int bpos = reader.capturedValues[pos++];
        int blen = reader.capturedValues[pos++];
        int bmsk = reader.capturedValues[pos++];
        
        trie.setValue(reader.capturedBlobArray, bpos, blen, bmsk, value);
 
    }
    
    public static <A extends Appendable> A capturedFieldBytesAsUTF8(TrieParserReader reader, int idx, A target) {
        
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int bpos = reader.capturedValues[pos++];
        int blen = reader.capturedValues[pos++];
        int bmsk = reader.capturedValues[pos++];
        
        return Appendables.appendUTF8(target, reader.capturedBlobArray, bpos, blen, bmsk);

    }
    
    public static <A extends Appendable> A capturedFieldBytesAsUTF8Debug(TrieParserReader reader, int idx, A target) {
        
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int bpos = reader.capturedValues[pos++];
        int blen = reader.capturedValues[pos++];
        int bmsk = reader.capturedValues[pos++];
        
        return Appendables.appendUTF8(target, reader.capturedBlobArray, bpos-10, blen+20, bmsk);

    }
        
    public static int writeCapturedUTF8ToPipe(TrieParserReader reader, Pipe<?> target, int idx, int loc) {
    	int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int bpos = reader.capturedValues[pos++];
        int blen = reader.capturedValues[pos++];
        PipeWriter.writeBytes(target, loc, reader.capturedBlobArray, bpos, blen, reader.capturedValues[pos++]);
        
        return blen;

    }
    
    
    public static <S extends MessageSchema<S>> int writeCapturedValuesToDataOutput(TrieParserReader reader, DataOutputBlobWriter<S> target, boolean writeIndex) throws IOException {
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
                DataOutputBlobWriter.write(target,reader.capturedBlobArray,p,l,m);
                                
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
                		//System.err.println("A write packed long "+value);
                	    if (writeIndex && !DataOutputBlobWriter.tryWriteIntBackData(target, writePosition)) {
                         	throw new IOException("Pipe var field length is too short for "+DataOutputBlobWriter.class.getSimpleName()+" change config for "+target.getPipe());
                        }                         
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
            
            if (writeIndex && !DataOutputBlobWriter.tryWriteIntBackData(target, writePosition)) {
            	throw new IOException("Pipe var field length is too short for "+DataOutputBlobWriter.class.getSimpleName()+" change config for pipe varLength is "+target.getPipe().maxVarLen);
            }
            
            
        }        

        return totalBytes;
    }

    public static long capturedDecimalMField(TrieParserReader reader, int idx) {
    	
           int pos = idx*4;
           
           long sign = reader.capturedValues[pos++];
           assert(sign!=0);      	
           return (long) ((((long)reader.capturedValues[pos++])<<32) | (0xFFFFFFFFL&reader.capturedValues[pos++]))*sign; 
    }
   
    public static byte capturedDecimalEField(TrieParserReader reader, int idx) {
    	
      	  int meta = reader.capturedValues[(idx*4)+3];
          return (meta<0) ? (byte) -(meta & 0xFFFF) : (byte)0;
   }
   
    
    
    public static long capturedLongField(TrieParserReader reader, int idx) {
    	
   	    int pos = idx*4;
        
        int sign = reader.capturedValues[pos++];
        assert(sign!=0);
   	
        long value = reader.capturedValues[pos++];
        value = (value<<32) | (0xFFFFFFFF&reader.capturedValues[pos++]);

        int meta = reader.capturedValues[pos];
      ///  byte dbase = (byte)((meta>>16)&0xFF);
//>>>>>>> heads/oci-pronghorn/master
		assert((meta&0xFFFF) != 0) : "No number was found, no digits were parsed.";

		return value*sign; //TODO: needs unit test to cover positive and negative numbers.
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
				Appendables.appendUTF8(target, reader.capturedBlobArray,p,l,m);

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
