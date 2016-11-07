package com.ociweb.pronghorn.util;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class TrieParserReader {
    
    private static final Logger logger = LoggerFactory.getLogger(TrieParserReader.class);
    
    private byte[] sourceBacking;
    public int    sourcePos;
    public  int    sourceLen;
    private int    sourceMask;
    
    
    private int[]  capturedValues;
    private int    capturedPos;
    private byte[] capturedBlobArray;
    
    private final int maxVarLength;
    private final int minVarLength;
    
    private long    safeReturnValue = -1;
    private int     safeCapturedPos = -1;
    private int     saveCapturedLen = -1;
    private int     safeSourcePos = -1;
    

    private final static int MAX_ALT_DEPTH = 256; //full recursion on alternate paths from a single point.
    private int altStackPos = 0;
    private int altStackExtractCount = 0;
    private int[] altStackA = new int[MAX_ALT_DEPTH];
    private int[] altStackB = new int[MAX_ALT_DEPTH];
    private int[] altStackC = new int[MAX_ALT_DEPTH];
    private int[] altStackD = new int[MAX_ALT_DEPTH];
    
    private short[] workingMultiStops = new short[MAX_ALT_DEPTH];
    private int[]   workingMultiContinue = new int[MAX_ALT_DEPTH];
    
    
    //TODO: when looking for N stops or them together as a quick way to avoid a number of checks.
       
    public void debug() {
        System.err.println(TrieParserReader.class.getName()+" reader debug() details:");
        System.err.println("pos  "+sourcePos+" masked "+(sourcePos&sourceMask));
        System.err.println("len  "+sourceLen);
        System.err.println("mask "+sourceMask);
        System.err.println("size "+sourceBacking.length);
        
    }
    
    public TrieParserReader() {
        this(0,1,65536);
    }
    
    public TrieParserReader(int maxCapturedFields) {
        this(maxCapturedFields*4,1,65536);
    }
    
    public TrieParserReader(int maxCapturedFields, int minVarLength, int maxVarLength) {
        this.capturedValues = new int[maxCapturedFields*4];
        this.minVarLength = minVarLength;
        this.maxVarLength = maxVarLength;
    }
    
    
    public void visit(TrieParser that, ByteSquenceVisitor visitor) {
        visit(that, 0, visitor);
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
    public void visit(TrieParser that, int i, ByteSquenceVisitor visitor) {
        
            switch (that.data[i]) {
                case TrieParser.TYPE_RUN:
                    
                    final int run = that.data[i+1];
                    final int idx = i + TrieParser.SIZE_OF_RUN;
                    
                    if (visitor.open(that.data, idx, run)) {
                    
                        visit(that, idx+run, visitor);
                        visitor.close(run);
                    }
                    
                    break;

                case TrieParser.TYPE_BRANCH_VALUE:
                    {
                        int localJump = i + TrieParser.SIZE_OF_BRANCH;
                        int farJump   = i + ((that.data[i+2]<<15) | (0x7FFF&that.data[i+3])); 
                        
                        visit(that, localJump, visitor);
                        visit(that, farJump, visitor);
                    }
                    break;
                case TrieParser.TYPE_ALT_BRANCH:
                    {
                        int localJump = i + TrieParser.SIZE_OF_ALT_BRANCH;
                        int farJump   = i + ((((int)that.data[i+2])<<15) | (0x7FFF&that.data[i+3])); 
                        
                        visit(that, localJump, visitor);
                        visit(that, farJump, visitor);
                    }   
                    break;

                case TrieParser.TYPE_VALUE_NUMERIC:
                    
                   
                    //take all the bytes that are ASCII numbers
                    //is no body here so i + 1 is next
                    //open int
                    //visit
                    //close int
                    
                    
                    break;

                case TrieParser.TYPE_VALUE_BYTES:
                    
                    //take all the bytes until we read the stop byte.
                    //is no body here so i + 2 is next
                    //open bytes
                    //visit
                    //close bytes
                    
                    break;
                case TrieParser.TYPE_SAFE_END:

                    visitor.end(
                            (0XFFFF&that.data[i+1])
                           ); 

                    break;
                case TrieParser.TYPE_END:

                        visitor.end(
                                (0XFFFF&that.data[i+1])
                               ); 

                    break;
                default:
                    throw new UnsupportedOperationException("ERROR Unrecognized value\n");
            }            
        
    }

    
    
    public static void parseSetup(TrieParserReader that, byte[] source, int offset, int length, int mask) {
        assert(length<=source.length) : "length is "+length+" but the array is only "+source.length;
        that.sourceBacking = source;
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
        } catch (IOException e) {
            e.printStackTrace();
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
        
        int debugPos = reader.sourcePos;
        int debugLen = reader.sourceLen;
                
        long result =  query(reader, trie, reader.sourceBacking, reader.sourcePos, reader.sourceLen, reader.sourceMask, -1);
        
        boolean debug = false;
        if (result==-1 && debug) {            
            reportError(reader, debugPos, debugLen);
        }
        
        return result;
                
    }

    private static void reportError(TrieParserReader reader, int debugPos, int debugLen) {
        String input = "";
  
        input = Appendables.appendUTF8(new StringBuilder(), 
                                       reader.sourceBacking, 
                                       debugPos, 
                                       Math.min(500,(int)debugLen), 
                                       reader.sourceMask).toString();


        
        
        System.out.println("pos:"+debugPos+" len:"+debugLen+" unable to parse:\n'"+input+"'");
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
    
    public static int parseCopy(TrieParserReader reader, long count, DataOutputBlobWriter<?> writer) {
        
    	int len = (int)Math.min(count, (long)reader.sourceLen);    	

    	DataOutputBlobWriter.write(writer, reader.sourceBacking, reader.sourcePos, len, reader.sourceMask);    	
    	reader.sourcePos += len;
        reader.sourceLen -= len;       
        
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
        
        initForQuery(reader, trie, source, sourcePos);
        
        boolean hasSafePoint = false;
                
        top:
        while (reader.type != TrieParser.TYPE_END) {  
            int t = reader.type;
            if (t==TrieParser.TYPE_BRANCH_VALUE) {    
                
                
//                if (0 == (0xFF & source[sourceMask & reader.localSourcePos] & trie.data[reader.pos++])) {
//                    int inc = ((((int)trie.data[reader.pos++])<<15) | (0x7FFF&trie.data[reader.pos]) + 1);
//                    reader.pos += inc; 
//                } else {
//                    reader.pos += 2;
//                }
                
                //TODO: we compute both sides of jump when we do not need to
                
                reader.pos = TrieParser.jumpOnBit((short) source[sourceMask & reader.localSourcePos], trie.data[reader.pos++], (((int)trie.data[reader.pos++])<<15) | (0x7FFF&trie.data[reader.pos]), reader.pos);
               
                
                
    //            short critera = trie.data[reader.pos++];
            
      //          reader.pos = 1 + (( (~((((short) source[sourceMask & reader.localSourcePos] & (0xFF & critera))-1)>>>8) ^ critera>>>8)) & ((((int)trie.data[reader.pos++])<<15) | (0x7FFF&trie.data[reader.pos]))) + reader.pos;
                
            
            
            } else if (t == TrieParser.TYPE_RUN) {                
                //run
                int run = trie.data[reader.pos++];        
                if (!(trie.skipDeepChecks && !hasSafePoint && 0==reader.altStackPos)) {
                    int r = scanForMismatch(reader, source, sourceMask, trie.data, run);
                    if (r>=0) {
                        if (hasSafePoint) {
                            return useSafePoint(reader);
                        } else {
                            if (reader.altStackPos > 0) {                                
                                tryNextChoiceOnStack(reader, trie.data);
                                continue top;                                
                            } else {
                                return unfoundResult;
                            }
                        }
                    }    
                } else {
                    reader.pos += run;
                    reader.localSourcePos += run; 
                }
                reader.runLength += run;
                
            } else if (t == TrieParser.TYPE_SAFE_END) {                    
                
                recordSafePointEnd(reader, reader.localSourcePos, reader.pos, trie);  
                hasSafePoint = true;
                reader.pos += trie.SIZE_OF_RESULT;
                if (sourceLength != reader.runLength) {
                } else {
                    return useSafePointNow(reader);
                }   
                
            } else if (t == TrieParser.TYPE_VALUE_BYTES) {
               
                parseBytes(reader, trie, source, sourceLength, sourceMask);
                
                if (reader.localSourcePos<0) {
                    if (reader.altStackPos > 0) {
                        //try other path
                        //reset all the values to the other path and continue from the top
                        
                        tryNextChoiceOnStack(reader, trie.data);
                        continue top;
                        
                    } else {
                        return unfoundResult;
                    }
                }
            } else if (t == TrieParser.TYPE_VALUE_NUMERIC) {       
            	
            	int temp = parseNumeric(reader,source,reader.localSourcePos, sourceLength-reader.runLength, sourceMask, (int)trie.data[reader.pos++]);
            	if (temp<0) {
            		return unfoundResult;
            	}            	
                reader.localSourcePos = temp;
                
            } else if (t == TrieParser.TYPE_ALT_BRANCH) {
                 processAltBranch(reader, trie.data);                 
            } else  {                
                logger.error(trie.toString());
                throw new UnsupportedOperationException("Bad jump length now at position "+(reader.pos-1)+" type found "+reader.type);
            }
           
            reader.type = trie.data[reader.pos++]; 
        }

        reader.sourceLen -= (reader.localSourcePos-reader.sourcePos);
        reader.sourcePos = reader.localSourcePos;
        
        return TrieParser.readEndValue(trie.data,reader.pos, trie.SIZE_OF_RESULT);
        
        
    }

    private static void parseBytes(TrieParserReader reader, TrieParser trie, byte[] source, long sourceLength,
            int sourceMask) {
        short stopValue = trie.data[reader.pos++];
        
        int stopCount = 0;
        
        reader.workingMultiContinue[stopCount] = reader.pos;
        reader.workingMultiStops[stopCount++] = stopValue;
                                
        if (reader.altStackPos>0) {
            stopCount = scanAllStackPos(reader, reader.localSourcePos, trie.data, reader.runLength, stopCount);
        }
        
        if (stopCount>1) {
            reader.localSourcePos = parseBytes(reader,source,reader.localSourcePos, sourceLength-reader.runLength, sourceMask, reader.workingMultiStops, stopCount);                    
            reader.pos = adjustConntinueFrom(reader, source, reader.localSourcePos, sourceMask, reader.pos, stopCount);
        } else {
            reader.localSourcePos = parseBytes(reader,source,reader.localSourcePos, sourceLength-reader.runLength, sourceMask, stopValue);
        }
    }

    private static void initForQuery(TrieParserReader reader, TrieParser trie, byte[] source, int sourcePos) {
        reader.capturedPos = 0;
        reader.capturedBlobArray = source;
        //working vars
        reader.pos = 0;
        reader.runLength = 0;
        reader.localSourcePos =sourcePos;
        
        reader.altStackPos = 0;
        reader.altStackExtractCount = 0;  
        
        assert(trie.getLimit()>0) : "SequentialTrieParser must be setup up with data before use.";
        
        reader.type = trie.data[reader.pos++];
    }

    private static void processAltBranch(TrieParserReader reader, short[] localData) {
        assert(localData[reader.pos]>=0): "bad value "+localData[reader.pos];
         assert(localData[reader.pos+1]>=0): "bad value "+localData[reader.pos+1];
             
         altBranch(localData, reader, reader.pos, reader.localSourcePos, (((int)localData[reader.pos++])<<15) | (0x7FFF&localData[reader.pos++]), localData[reader.pos], reader.runLength); 
 
         
         //pop off the top of the stack and use that value.
         reader.localSourcePos     = reader.altStackA[--reader.altStackPos];
         reader.capturedPos = reader.altStackB[reader.altStackPos];
         reader.pos         = reader.altStackC[reader.altStackPos];
         reader.runLength   = reader.altStackD[reader.altStackPos];
    }

    private static long useSafePointNow(TrieParserReader reader) {
        //hard stop passed in forces us to use the safe point
        reader.sourceLen -= (reader.localSourcePos-reader.sourcePos);
        reader.sourcePos = reader.localSourcePos;
        return reader.safeReturnValue;
    }

    private static void tryNextChoiceOnStack(TrieParserReader reader, short[] localData) {
        //try other path
        //reset all the values to the other path and continue from the top
        
        reader.localSourcePos     = reader.altStackA[--reader.altStackPos];
        reader.capturedPos = reader.altStackB[reader.altStackPos];
        reader.pos         = reader.altStackC[reader.altStackPos];
        reader.runLength = reader.altStackD[reader.altStackPos];                                
        
        reader.type = localData[reader.pos++];
    }

    private static int scanForMismatch(TrieParserReader reader, byte[] source, int sourceMask, short[] localData,
            int run) {
   
        int r = run;
        while ((--r >= 0) && (localData[reader.pos++] == source[sourceMask & reader.localSourcePos++]) ) {
        }
        return r;
    }

    private static long useSafePoint(TrieParserReader reader) {
        reader.localSourcePos  = reader.safeSourcePos;
        reader.capturedPos = reader.safeCapturedPos;
        reader.sourceLen = reader.saveCapturedLen;
        reader.sourcePos =reader.localSourcePos;
        
        return reader.safeReturnValue;
    }

    private static int scanAllStackPos(TrieParserReader reader, int localSourcePos, short[] localData, int runLength,
            int stopCount) {
        int i = reader.altStackPos;  //TODO: fix this method is taking 20% of load.
         while (--i>=0) {
            if (localData[reader.altStackC[i]] == TrieParser.TYPE_VALUE_BYTES) {
                short newStop = localData[reader.altStackC[i]+1];
                
                if (reader.capturedPos != reader.altStackB[i]) {//part of the same path.
                    //System.out.println("a");
                    break;
                }
                if (localSourcePos != reader.altStackA[i]) {//part of the same path.
                   // System.out.println("b");
                    break;
                }
                if (runLength != reader.altStackD[i]){
                   // System.out.println("c");
                    break;
                }
                if (!equalsNone(newStop, reader.workingMultiStops, stopCount)) {
                    //System.out.println("d");
                    break;//safety until the below issue detected by the assert is fixed.
                }
                
                //TODO: insert must branch after matching byte extract NOT before
                //assert(equalsNone(newStop, reader.workingMultiStops, stopCount)) : "Trie did not insert correctly, should only have one of these.";
                
                reader.workingMultiContinue[stopCount] = reader.altStackC[i]+2;
                reader.workingMultiStops[stopCount++] = newStop;
                
                //taking this one
                reader.altStackPos--;
                
            }
        }
        return stopCount;
    }

    private static int adjustConntinueFrom(TrieParserReader reader, byte[] source, int localSourcePos, int sourceMask,
            int pos, int stopCount) {
        if (localSourcePos>=0) {
            //determine where to now continue from
            short selected = source[sourceMask&(localSourcePos-1)];
            int j = stopCount;
            while (--j>=0) {
                if (selected== reader.workingMultiStops[j]) {
                    pos = reader.workingMultiContinue[j];
                    break;
                }
            }
            assert(j>=0):"should have found value";
        }
        return pos;
    }

    static void recurseAltBranch(short[] localData, TrieParserReader reader, int pos, int offset, int runLength) {
        int type = localData[pos];
        if (type == TrieParser.TYPE_ALT_BRANCH) {
            
            pos++;
            if (1 != TrieParser.BRANCH_JUMP_SIZE ) {
                assert(localData[pos]>=0): "bad value "+localData[pos];
                assert(localData[pos+1]>=0): "bad value "+localData[pos+1];
                
                altBranch(localData, reader, pos, offset, (((int)localData[pos++])<<15) | (0x7FFF&localData[pos++]), localData[pos], runLength); 
            } else {
                altBranch(localData, reader, pos, offset, localData[pos++], localData[pos], runLength);                                   
            }
            
        } else {
            
            pushAlt(reader, pos, offset, runLength);
//            if (type == TrieParser.TYPE_VALUE_BYTES) {
//                
//                int j = 0;//TODO: can replace with keeping track of this value instead of scanning for it.
//                while (j< reader.altStackPos ) {
//                    if (localData[reader.altStackC[j]] != TrieParser.TYPE_VALUE_BYTES){
//                        break;
//                    }
//                    j++;
//                }
//                
//                if (j<reader.altStackPos) {
//                    
//                    System.out.println("now tested:"+j+" "+reader.altStackExtractCount);
//                                        
//                    assert(j==reader.altStackExtractCount);
//                    
//                    //swap j with reader.altStackPos-1;
//                    int k = reader.altStackPos-1;
//                 
//                    int a = reader.altStackA[k];
//                    int b = reader.altStackB[k];
//                    int c = reader.altStackC[k];
//                    
//                    reader.altStackA[j] = a;
//                    reader.altStackB[j] = b;
//                    reader.altStackC[j] = c;
//                            
//                    reader.altStackExtractCount++;
//                }
//                //TODO: when the top of the stack is a bytes extract keep peeking and take all the stop values together.
//                
//            }
            
            
            
            
            
        }
    }
    
    static void altBranch(short[] localData, TrieParserReader reader, int pos, int offset, int jump, int peekNextType, int runLength) {
        assert(jump>0) : "Jump must be postitive but found "+jump;
        
        //put extract first so its at the bottom of the stack
        if (TrieParser.TYPE_VALUE_BYTES == peekNextType || TrieParser.TYPE_VALUE_NUMERIC==peekNextType) {
            //Take the Jump value first, the local value has an extraction.
            //push the LocalValue
            recurseAltBranch(localData, reader, pos+ TrieParser.BRANCH_JUMP_SIZE, offset, runLength);
            recurseAltBranch(localData, reader, pos+jump+ TrieParser.BRANCH_JUMP_SIZE, offset, runLength);           
        } else {
            //Take the Local value first
            //push the JumpValue
            recurseAltBranch(localData, reader, pos+jump+ TrieParser.BRANCH_JUMP_SIZE, offset, runLength);
            recurseAltBranch(localData, reader, pos+ TrieParser.BRANCH_JUMP_SIZE, offset, runLength);
        }
    }

    static void pushAlt(TrieParserReader reader, int pos, int offset, int runLength) {
        
        reader.altStackA[reader.altStackPos] = offset;
        reader.altStackB[reader.altStackPos] = reader.capturedPos;
        reader.altStackC[reader.altStackPos++] = pos;        
        reader.altStackD[reader.altStackPos] = runLength;
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
        int lim = (int)Math.min(remainingLen, sourceMask);
        do {
        } while (--lim >= 0 && stopValue!=source[sourceMask & x++]);         
        int len = (x-sourcePos)-1;
                
        if (lim<0) {//not found!
            return -1;
        }
        reader.capturedPos = extractedBytesRange(reader.capturedValues, reader.capturedPos, sourcePos, len, sourceMask);                
        return x;
    }
    
    private static int parseBytes(TrieParserReader reader, final byte[] source, final int sourcePos, long remainingLen, final int sourceMask, final short[] stopValues, final int stopValuesCount) {              

        int x = sourcePos;
        int lim = (int)Math.min(remainingLen, sourceMask);
        StringBuilder b=new StringBuilder();
        do {
            if (lim>0) {
                b.append((char)source[sourceMask & x]);
            }
            
        } while (--lim >= 0 && equalsNone(source[sourceMask & x++], stopValues, stopValuesCount ) );         
        int len = (x-sourcePos)-1;
                
        if (lim<0) {//not found!
            int z = (int)Math.min(remainingLen, sourceMask);
 //           System.out.println("not found in scan of "+z+"  "+b);
            return -1;
        }
        reader.capturedPos = extractedBytesRange(reader.capturedValues, reader.capturedPos, sourcePos, len, sourceMask);                
        return x;
    }
    
    private static boolean equalsNone(short value, short[] data, int count) {
        int i = count;
        while (--i>=0) {
            if (value == data[i]) {
                return false;
            }
        }
        return true;
    }

    private static int extractedBytesRange(int[] target, int pos, int sourcePos, int sourceLen, int sourceMask) {
        target[pos++] = 0;  //this flag tells us that these 4 values are not a Number but instead captured Bytes
        target[pos++] = sourcePos;
        target[pos++] = sourceLen;
        target[pos++] = sourceMask;
        return pos;
    }
    
    private static int parseNumeric(TrieParserReader reader, byte[] source, int sourcePos, long sourceLength, int sourceMask, int numType) {
        
        byte sign = 1;
        long intValue = 0;
        byte intLength = 0;
        byte base=10;
                
        
        if (0!= (TrieParser.NUMERIC_FLAG_DECIMAL&numType)) {
            final short c = source[sourceMask & sourcePos];
            if ('.'!=c) {
                publish(reader, 1, 0, 1, 10);
                //do not parse numeric
                return sourcePos;
            } else {
                sourcePos++;
            }
            
        } else if (0!= (TrieParser.NUMERIC_FLAG_RATIONAL&numType)) {
            final short c = source[sourceMask & sourcePos];
            if ('/'!=c) {
                publish(reader, 1, 1, 1, 10);
                //do not parse numeric
                return sourcePos;
            } else {
                sourcePos++;
            }
            
        }
        
        //NOTE: these Numeric Flags are invariants consuming runtime resources, this tree could be pre-compiled to remove them if neded.
        if (0!=(TrieParser.NUMERIC_FLAG_SIGN&numType)) {
            final short c = source[sourceMask & sourcePos];
            if (c=='-') { //NOTE: check ASCII table there may be a fater way to do this.
                sign = -1;
                sourcePos++;
            }
            if (c=='+') {
                sourcePos++;
            }
        }
        
        //just to keep it from spinning on values that are way out of bounds
        sourceLength = Math.min(32, sourceLength); //never scan over 32
        
        boolean hasNo0xPrefix = ('0'!=source[sourceMask & sourcePos+1]) || ('x'!=source[sourceMask & sourcePos+2]);
		if (hasNo0xPrefix && 0==(TrieParser.NUMERIC_FLAG_HEX&numType) ) {                            
            base = 10;
            short c = 0;
            do {
                c = source[sourceMask & sourcePos++];                    
                if ((c>='0') && (c<='9') && intLength<sourceLength) {
                    intValue = (intValue * 10)+(c-'0');
                    intLength++;
                    continue;
                } else {
                    break;
                }
            }  while (true);
            if (intLength>19) {
                //ERROR
                
            }
        } else {
            base = 16;
            if (!hasNo0xPrefix) {
            	sourcePos+=2;//skipping over the 0x checked above
            }
            short c = 0;
            do {
                c = source[sourceMask & sourcePos++];
                
                if ((c>='0') && (c<='9') && intLength<sourceLength) {
                    intValue = (intValue<<4)+(c-'0');
                    intLength++;
                    continue;
                } else  if ((c>='a') && (c<='f') && intLength<sourceLength) {
                    intValue = (intValue<<4)+(10+(c-'a'));
                    intLength++;
                    continue;
                } else {
                    break;
                }
            }  while (true);
            
            if (intLength>16) {
                //ERROR
            	return -1;
            }
            
        }

		if (intLength==0) {
			return -1;
		}
        publish(reader, sign, intValue, intLength, base);
        
        return sourcePos-1;
    }

    private static void publish(TrieParserReader reader, int sign, long numericValue, int intLength, int base) {
        assert(0!=sign);
        
        reader.capturedValues[reader.capturedPos++] = sign;
        reader.capturedValues[reader.capturedPos++] = (int) (numericValue >> 32);
        reader.capturedValues[reader.capturedPos++] = (int) (0xFFFFFFFF &numericValue);
        reader.capturedValues[reader.capturedPos++] = (base<<16) | (0xFFFF & intLength) ; //Base: 10 or 16, IntLength:  
        
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
    
    
    public static long capturedLongField(TrieParserReader reader, int idx) {
    	
    	 int pos = idx*4;
         
         int sign = reader.capturedValues[pos++];
         assert(sign!=0);
    	
         long value = reader.capturedValues[pos++];
         value = (value<<32) | (0xFFFFFFFF&reader.capturedValues[pos++]);

         assert((reader.capturedValues[pos]&0xFFFF) != 0) : "No number was found, no digits were parsed.";
         
         return value;
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
    
    public static <A extends Appendable> A capturedFieldBytesAsUTF8(TrieParserReader reader, int idx, A target) {
        
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int bpos = reader.capturedValues[pos++];
        int blen = reader.capturedValues[pos++];
        int bmsk = reader.capturedValues[pos++];
        
        return Appendables.appendUTF8(target, reader.capturedBlobArray, bpos, blen, bmsk);


    }
   
    public static int writeCapturedValuesToPipe(TrieParserReader reader, Pipe<?> target) {
        int limit = reader.capturedPos;
        int[] localCapturedValues = reader.capturedValues;
        
        
        int totalBytes = 0;
        int i = 0;
        while (i < limit) {
            
            int type = localCapturedValues[i++];
            
            if (0==type) {
                
                int p = localCapturedValues[i++];
                int l = localCapturedValues[i++];
                int m = localCapturedValues[i++];   
                
                
                totalBytes += l;
                Pipe.addByteArrayWithMask(target, m, l, reader.capturedBlobArray, p);
                
            } else {
                
                Pipe.addIntValue(type, target);
                Pipe.addIntValue(localCapturedValues[i++], target);
                Pipe.addIntValue(localCapturedValues[i++], target);
                Pipe.addIntValue(localCapturedValues[i++], target);
                
            }            
        }
        return totalBytes;
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
    
    
    public static int writeCapturedValuesToDataOutput(TrieParserReader reader, DataOutputBlobWriter target) throws IOException {
        int limit = reader.capturedPos;
        int[] localCapturedValues = reader.capturedValues;
        
        
        int totalBytes = 0;
        int i = 0;
        while (i < limit) {
            
            int type = localCapturedValues[i++];
            
            if (0==type) {
                
                int p = localCapturedValues[i++];
                int l = localCapturedValues[i++];
                int m = localCapturedValues[i++];   
                                
                totalBytes += l;
                
                //if those bytes were utf8 encoded then this matches the same as writeUTF8 without decode/encode
                target.writeShort(l);
                
         //       logger.info("captured text: {}", Appendables.appendUTF8(new StringBuilder(), reader.capturedBlobArray, p, l, m));
                
                
                DataOutputBlobWriter.write(target,reader.capturedBlobArray,p,l,m);
                                
            } else {
                
                target.writeInt(type);
                target.writeInt(localCapturedValues[i++]);
                target.writeInt(localCapturedValues[i++]);
                target.writeInt(localCapturedValues[i++]);
                
            }            
        }
        return totalBytes;
    }
    
    public static int writeCapturedValuesToAppendable(TrieParserReader reader, Appendable target) throws IOException {
        int limit = reader.capturedPos;
        int[] localCapturedValues = reader.capturedValues;
        
        
        int totalBytes = 0;
        int i = 0;
        while (i < limit) {
            
            int type = localCapturedValues[i++];
            
            if (0==type) {
                
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
    
    //this is only for single fields that appear out of order and need to be put back in order.
    public static void writeCapturedValuesToPipe(TrieParserReader reader, Pipe<?> target, long baseSlabPosition) {
        int limit = reader.capturedPos;
        int[] localCapturedValues = reader.capturedValues;

        int i = 0;
        while (i < limit) {
            
            int type = localCapturedValues[i++];
            
            if (0==type) {
                
                int p = localCapturedValues[i++];
                int l = localCapturedValues[i++];
                int m = localCapturedValues[i++];   
                Pipe.setByteArrayWithMask(target, m, l, reader.capturedBlobArray, p, baseSlabPosition);
                
            } else {
                Pipe.setIntValue(type, target, baseSlabPosition++);
                Pipe.setIntValue(localCapturedValues[i++], target, baseSlabPosition++);
                Pipe.setIntValue(localCapturedValues[i++], target, baseSlabPosition++);
                Pipe.setIntValue(localCapturedValues[i++], target, baseSlabPosition++);
            }
            
        }
        
    }
    
    

}
