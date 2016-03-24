package com.ociweb.pronghorn.util;

import java.io.DataOutput;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;

public class SequentialTrieParserReader {
    
    private static final Logger logger = LoggerFactory.getLogger(SequentialTrieParserReader.class);
    
    private byte[] sourceBacking;
    private int    sourcePos;
    private int    sourceLen;
    private int    sourceMask;
    
    
    private int[]  capturedValues;
    private int    capturedPos;
    private byte[] capturedBlobArray;
    
    private boolean hasSafePoint = false;
    private long    safeReturnValue = -1;
    private int     safeCapturedPos = -1;
    private int     safeSourcePos = -1;
    
    private int pos = 0;

    private final static int MAX_ALT_DEPTH = 32;
    private int altStackPos = 0;
    private int[] altStackA = new int[MAX_ALT_DEPTH];
    private int[] altStackB = new int[MAX_ALT_DEPTH];
    private int[] altStackC = new int[MAX_ALT_DEPTH];
    
    
    public SequentialTrieParserReader() {
        this(0);
    }
    
    public SequentialTrieParserReader(int maxCapturedFields) {
        this.capturedValues = new int[maxCapturedFields*4];
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
     * 
     *  
     * 
     */
    public void visit(SequentialTrieParser that, int i, ByteSquenceVisitor visitor) {
        
            switch (that.data[i]) {
                case SequentialTrieParser.TYPE_RUN:
                    
                    final int run = that.data[i+1];
                    final int idx = i + SequentialTrieParser.SIZE_OF_RUN;
                    
                    if (visitor.open(that.data, idx, run)) {
                    
                        visit(that, idx+run, visitor);
                        visitor.close(run);
                    }
                    
                    break;

                case SequentialTrieParser.TYPE_BRANCH_VALUE:
                    {
                        int localJump = i + SequentialTrieParser.SIZE_OF_BRANCH;
                        int farJump   = i + ((that.data[i+2]<<16) | (0xFFFF&that.data[i+3])); 
                        
                        visit(that, localJump, visitor);
                        visit(that, farJump, visitor);
                    }
                    break;
                case SequentialTrieParser.TYPE_ALT_BRANCH:
                    {
                        int localJump = i + SequentialTrieParser.SIZE_OF_ALT_BRANCH;
                        int farJump   = i + ((that.data[i+2]<<16) | (0xFFFF&that.data[i+3])); 
                        
                        visit(that, localJump, visitor);
                        visit(that, farJump, visitor);
                    }   
                    break;

                case SequentialTrieParser.TYPE_VALUE_NUMERIC:
                    
                   
                    //take all the bytes that are ASCII numbers
                    //is no body here so i + 1 is next
                    //open int
                    //visit
                    //close int
                    
                    
                    break;

                case SequentialTrieParser.TYPE_VALUE_BYTES:
                    
                    //take all the bytes until we read the stop byte.
                    //is no body here so i + 2 is next
                    //open bytes
                    //visit
                    //close bytes
                    
                    break;
                case SequentialTrieParser.TYPE_SAFE_END:

                    visitor.end(
                            (0XFFFF&that.data[i+1])
                           ); 

                    break;
                case SequentialTrieParser.TYPE_END:

                        visitor.end(
                                (0XFFFF&that.data[i+1])
                               ); 

                    break;
                default:
                    throw new UnsupportedOperationException("ERROR Unrecognized value\n");
            }            
        
    }

    
    
    public static void parseSetup(SequentialTrieParserReader that, byte[] source, int offset, int length, int mask) {
        if (null==source) {
            throw new NullPointerException();
        }
        that.sourceBacking = source;
        that.sourcePos     = offset;
        that.sourceLen     = length;
        that.sourceMask    = mask;        
        
    }
    
    public static boolean parseHasContent(SequentialTrieParserReader reader) {
        return reader.sourceLen>0;
    }
    
    public static long parseNext(SequentialTrieParserReader reader, SequentialTrieParser trie) {

        //logNextTextToParse(reader);
        
        return query(reader, trie, reader.sourceBacking, reader.sourcePos, reader.sourceLen, reader.sourceMask);
                
    }

    private static void logNextTextToParse(SequentialTrieParserReader reader) {
        StringBuilder builder = new StringBuilder();
        int c = Math.min(40,reader.sourceLen);
        int p = reader.sourcePos;
        while (--c>=0) {
            builder.append((char)reader.sourceBacking[reader.sourceMask & p++]);
        }
        String toParse = builder.toString();
        logger.warn("to parse next:{}",toParse);
    }
    
    public static void parseSkip(SequentialTrieParserReader reader, int count) {
        reader.sourcePos += count;
        reader.sourceLen -= count;
    }
    
    /**
     * Gather until stop value is reached.
     * @param reader
     * @param stop
     */
    public static void parseGather(SequentialTrieParserReader reader, DataOutput output, final byte stop) {
        
        byte[] source = reader.sourceBacking;
        int    mask   = reader.sourceMask;
        int    pos    = reader.sourcePos;
        int    len    = reader.sourceLen;
        
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
    
    public static void parseGather(SequentialTrieParserReader reader, final byte stop) {
        
        byte[] source = reader.sourceBacking;
        int    mask   = reader.sourceMask;
        int    pos    = reader.sourcePos;
        int    len    = reader.sourceLen;
        
        byte   value;        
        while(stop != (value=source[mask & pos++]) ) {
        }
        reader.sourcePos = pos;
        
    }    

    public static long query(SequentialTrieParserReader trieReader, SequentialTrieParser trie, Pipe<?> input) {
        int meta = Pipe.takeRingByteMetaData(input);
        int length    = Pipe.takeRingByteLen(input);
        return query(trieReader, trie, Pipe.byteBackingArray(meta, input), Pipe.bytePosition(meta, input, length), length, Pipe.blobMask(input)  );  
    }
    
    public static long query(SequentialTrieParserReader reader, SequentialTrieParser trie, 
                            byte[] source, int localSourcePos, int sourceLength, int sourceMask) {
        
        reader.capturedPos = 0;
        reader.capturedBlobArray = source;

        reader.pos = 0;
        
        reader.hasSafePoint = false;
        reader.safeReturnValue = -1;
        reader.safeCapturedPos = -1;
        
        reader.altStackPos = 0;
          
        short[] localData = trie.data;
        int runLength = 0;
        int type = localData[reader.pos++];
                
        top:
        while (type != SequentialTrieParser.TYPE_END) {
            
            if (type==SequentialTrieParser.TYPE_BRANCH_VALUE) {
                
                reader.pos = SequentialTrieParser.jumpOnBit((short) source[sourceMask & localSourcePos], reader.pos, localData);
                
            } else if (type == SequentialTrieParser.TYPE_RUN) {
                
                //run
                int run = localData[reader.pos++];
        
                if (SequentialTrieParser.skipDeepChecks && !reader.hasSafePoint && 0==reader.altStackPos) {
                    reader.pos += run;
                    localSourcePos += run;
                } else {
                    
                    int r = run;
                    int localP = reader.pos;
                    while ((--r >= 0) && (localData[localP++] == source[sourceMask & localSourcePos++]) ) {
                    }
                    reader.pos = localP;
                    if (r>=0) {
                        if (reader.hasSafePoint) {     
                            localSourcePos = restoreSafePoint(reader);
                            return reader.safeReturnValue;
                        } else {
                            if (reader.altStackPos > 0) {
                                //try other path
                                //reset all the values to the other path and continue from the top
                                localSourcePos = popAlt(reader);
                                type = localData[reader.pos++];
                                continue top;
                                
                            } else {
                                throw new RuntimeException("check prev branch, no match at pos "+reader.pos+"  \n"+trie);
                            }
                        }
                    }    
                    
                }
                runLength += run;
                
            } else if (type == SequentialTrieParser.TYPE_VALUE_BYTES) {
                
                //TODO: ERROR: when we inserted at front it should have started with an ALT not an extract.
                
//                EXTRACT_BYTES5[0], 10[1], 
//                SAFE6[2], 31[3], 
//                RUN0[4], 7[5], 0[6], 0[7], 0[8], 0[9], 0[10], 0[11], 0[12], 
//                SAFE6[13], 18[14], 
                //        System.out.println(trie);
                
                //         System.out.println("Apos "+reader.sourcePos+"  "+reader.pos);
                
                localSourcePos = parseBytes(reader,source,localSourcePos, sourceLength-runLength, sourceMask, localData[reader.pos++]);
                
                //         System.out.println("Bpos "+reader.sourcePos+"  "+reader.pos);
                
            } else if (type == SequentialTrieParser.TYPE_VALUE_NUMERIC) {
                
                parseNumeric(reader,source,localSourcePos, sourceLength-runLength, sourceMask, (int)localData[reader.pos++]);
                
            } else if (type == SequentialTrieParser.TYPE_SAFE_END) {                    
                
                recordSafePointEnd(reader, trie);                    
                if (sourceLength == runLength) {
                    //hard stop passed in forces us to use the safe point
                    reader.sourceLen -= (localSourcePos-reader.sourcePos);
                    reader.sourcePos = localSourcePos;
                    return reader.safeReturnValue;
                }                    

            } else if (type == SequentialTrieParser.TYPE_ALT_BRANCH) {

                reader.pos = altBranch(reader, localData, localSourcePos);                                   
                
            } else  {
                System.out.println(trie);
                throw new UnsupportedOperationException("Bad jump length now at position "+(reader.pos-1)+" type found "+type);
            }
            
            type = localData[reader.pos++]; 
        }
        
        reader.sourceLen -= (localSourcePos-reader.sourcePos);
        reader.sourcePos = localSourcePos;
        
        return SequentialTrieParser.readEndValue(localData,reader.pos, trie.SIZE_OF_RESULT);
        
        
    }

    private static int restoreSafePoint(SequentialTrieParserReader reader) {
        int localSourcePos;
        localSourcePos   = reader.safeSourcePos;
        reader.capturedPos = reader.safeCapturedPos;
        reader.sourceLen -= (localSourcePos-reader.sourcePos); 
        reader.sourcePos = localSourcePos;
        return localSourcePos;
    }

    private static int popAlt(SequentialTrieParserReader reader) {
        int localSourcePos;
        localSourcePos     = reader.altStackA[--reader.altStackPos];
        reader.capturedPos = reader.altStackB[reader.altStackPos];
        reader.pos         = reader.altStackC[reader.altStackPos];
        return localSourcePos;
    }

    static int altBranch(SequentialTrieParserReader reader, short[] data, int offset) {
        return altBranch(reader, reader.pos, offset, data[reader.pos++], data[reader.pos]);
    }

    static int altBranch(SequentialTrieParserReader reader, int pos, int offset, int jump, int peekNextType) {
        if (SequentialTrieParser.TYPE_VALUE_BYTES == peekNextType || SequentialTrieParser.TYPE_VALUE_NUMERIC==peekNextType) {
            //Take the Jump value first, the local value has an extraction.
            //push the LocalValue
            pushAlt(reader, pos+1, offset);
            pos+= (1+jump);//TODO: change this and the other occurrence so jump is 1 bigger and we avoid this add.            
        } else {
            //Take the Local value first
            //push the JumpValue
            pushAlt(reader, pos+jump+1, offset);
            pos+= 1;
        }
        return pos;
    }

    static void pushAlt(SequentialTrieParserReader reader, int pos, int offset) {
        
        reader.altStackA[reader.altStackPos] = offset;
        reader.altStackB[reader.altStackPos] = reader.capturedPos;
        reader.altStackC[reader.altStackPos++] = pos;        
    }

    private static void recordSafePointEnd(SequentialTrieParserReader reader, SequentialTrieParser trie) {
        reader.hasSafePoint = true;
        reader.safeReturnValue = SequentialTrieParser.readEndValue(trie.data,reader.pos, trie.SIZE_OF_RESULT);
        reader.safeCapturedPos = reader.capturedPos;
        
        reader.safeSourcePos = reader.sourcePos;        
        
        reader.pos += trie.SIZE_OF_RESULT;
        //if the following does not match we will return this safe value.
        //we do not yet have enough info to decide if this is the end or not.
    }

    private static int parseBytes(SequentialTrieParserReader reader, final byte[] source, final int sourcePos, int remainingLen, final int sourceMask, final short stopValue) {              
        
        int x = sourcePos;
     //   int z = sourceLengh;
      //  if (sourceLengh>z) {            
            do {
            } while (--remainingLen >= 0 && stopValue!=source[sourceMask & x++]);         
        //    if (z >= 0) {
                reader.capturedPos = extractedBytesRange(reader.capturedValues, reader.capturedPos, sourcePos, (x-sourcePos)-1, sourceMask);                
                return x;
       //     }
       // } 
     //   return parseBytesImpl(reader, source, sourcePos, sourceMask, stopValue, sourcePos, sourcePos + sourceLengh + 1);
  
    }
//
//    private static int parseBytesImpl(SequentialTrieParserReader reader, byte[] source, final int sourcePos, final int sourceMask, final short stopValue, int x, final int limit) {
//                
//        do {
//        } while (stopValue!=source[sourceMask & x++] && x<limit);
//        //x is always 2 past the last good value so we -1 to get back to the expected limit.
//                      
//       reader.capturedPos = extractedBytesRange(reader.capturedValues, reader.capturedPos, sourcePos, (x-sourcePos)-1, sourceMask);
//                
//        return x;
//    }

    private static int extractedBytesRange(int[] target, int pos, int sourcePos, int sourceLen, int sourceMask) {
        target[pos++] = 0;  //this flag tells us that these 4 values are not a Number but instead captured Bytes
        target[pos++] = sourcePos;
        target[pos++] = sourceLen;
        target[pos++] = sourceMask;
        return pos;
    }
    
    private static int parseNumeric(SequentialTrieParserReader reader, byte[] source, int sourcePos, int sourceLength, int sourceMask, int numType) {
        
        byte sign = 1;
        long intValue = 0;
        byte intLength = 0;
        byte base=10;
        
        
        if (0!= (SequentialTrieParser.NUMERIC_FLAG_DECIMAL&numType)) {
            final short c = source[sourceMask & sourcePos];
            if ('.'!=c) {
                publish(reader, 1, 0, 1, 10);
                //do not parse numeric
                return sourcePos;
            } else {
                sourcePos++;
            }
            
        } else if (0!= (SequentialTrieParser.NUMERIC_FLAG_RATIONAL&numType)) {
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
        if (0!=(SequentialTrieParser.NUMERIC_FLAG_SIGN&numType)) {
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
        sourceLength = 0x1F & sourceLength; //never scan over 32
        
        if (0==(SequentialTrieParser.NUMERIC_FLAG_HEX&numType) | ('0'!=source[sourceMask & sourcePos+1])| ('x'!=source[sourceMask & sourcePos+2])  ) {                            
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
            sourcePos+=2;//skipping over the 0x checked above
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
            }
            
        }

        publish(reader, sign, intValue, intLength, base);

        return sourcePos;
    }

    private static void publish(SequentialTrieParserReader reader, int sign, long numericValue, int intLength, int base) {
        assert(0!=sign);
        
        reader.capturedValues[reader.capturedPos++] = sign;
        reader.capturedValues[reader.capturedPos++] = (int) (numericValue >> 32);
        reader.capturedValues[reader.capturedPos++] = (int) (0xFFFFFFFF &numericValue);
        reader.capturedValues[reader.capturedPos++] = (base<<16) | (0xFFFF & intLength) ; //Base: 10 or 16, IntLength:  
        
    }


    public static void parseSetup(SequentialTrieParserReader trieReader, Pipe<?> input) {
        int meta = Pipe.takeRingByteMetaData(input);
        int length    = Pipe.takeRingByteLen(input);
        parseSetup(trieReader, Pipe.byteBackingArray(meta, input), Pipe.bytePosition(meta, input, length), length, Pipe.blobMask(input));
        
        //logger.warn("TO Parse:{}",Pipe.readASCII(input, new StringBuilder(), meta, length));
        
    }
    
    public static int capturedFieldCount(SequentialTrieParserReader reader) {
        return reader.capturedPos>>2;
    }
    
    public static void capturedFieldInts(SequentialTrieParserReader reader, int idx, int[] targetArray, int targetPos) {
        
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type!=0);
        targetArray[targetPos++] = type;
        targetArray[targetPos++] = reader.capturedValues[pos++];
        targetArray[targetPos++] = reader.capturedValues[pos++];
        targetArray[targetPos++] = reader.capturedValues[pos++];
        
    }
    
    public static int capturedFieldBytes(SequentialTrieParserReader reader, int idx, byte[] target, int targetPos, int targetMask) {
        
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int p = reader.capturedValues[pos++];
        int l = reader.capturedValues[pos++];
        int m = reader.capturedValues[pos++];

        Pipe.copyBytesFromToRing(reader.capturedBlobArray, p, m, target, targetPos, targetMask, l);
        
        return l;
    }
    
    public static int capturedFieldByte(SequentialTrieParserReader reader, int idx, int offset) {
        
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
    
    public static <A extends Appendable> A capturedFieldBytesAsUTF8(SequentialTrieParserReader reader, int idx, A target) {
        
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int bpos = reader.capturedValues[pos++];
        int blen = reader.capturedValues[pos++];
        int bmsk = reader.capturedValues[pos++];
        
        try {
            return Appendables.appendUTF8(target, reader.capturedBlobArray, bpos, blen, bmsk);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
   
    public static void writeCapturedValuesToPipe(SequentialTrieParserReader reader, Pipe<?> target) {
        int limit = reader.capturedPos;
        int[] localCapturedValues = reader.capturedValues;
        
        
        int i = 0;
        while (i < limit) {
            
            int type = localCapturedValues[i++];
            
            if (0==type) {
                
                int p = localCapturedValues[i++];
                int l = localCapturedValues[i++];
                int m = localCapturedValues[i++];   
                Pipe.addByteArrayWithMask(target, m, l, reader.capturedBlobArray, p);
                
            } else {
                
                Pipe.addIntValue(type, target);
                Pipe.addIntValue(localCapturedValues[i++], target);
                Pipe.addIntValue(localCapturedValues[i++], target);
                Pipe.addIntValue(localCapturedValues[i++], target);
                
            }            
        }
        
    }
    
    //this is only for single fields that appear out of order and need to be put back in order.
    public static void writeCapturedValuesToPipe(SequentialTrieParserReader reader, Pipe<?> target, long baseSlabPosition) {
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
