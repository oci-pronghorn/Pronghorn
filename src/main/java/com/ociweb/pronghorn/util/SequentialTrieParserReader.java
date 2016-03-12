package com.ociweb.pronghorn.util;

import java.io.DataOutput;
import java.io.IOException;

import com.ociweb.pronghorn.pipe.Pipe;

public class SequentialTrieParserReader {
    
    private byte[] sourceBacking;
    private int    sourcePos;
    private int    sourceLen;
    private int    sourceMask;
    
    
    private int[]  capturedValues;
    private int    capturedPos;
    private byte[] capturedBlobArray;
    
    private boolean hasSafePoint = false;
    private int     safeReturnValue = -1;
    private int     safeCapturedPos = -1;
    private int     safeSourcePos = -1;
    
    private int pos = 0;
    private int runLength = 0;
    private int altStackPos = 0;
    private int[] altStackA = new int[32];
    private int[] altStackB = new int[32];
    private int[] altStackC = new int[32];
    
    
    public SequentialTrieParserReader() {

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
                case SequentialTrieParser.TYPE_SAFE_END:

                        visitor.end(
                                (0XFFFF&that.data[i+1])
                               ); 

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
                case SequentialTrieParser.TYPE_RUN:
                    
                    final int run = that.data[i+1];
                    final int idx = i + SequentialTrieParser.SIZE_OF_RUN;
                    
                    if (visitor.open(that.data, idx, run)) {
                    
                        visit(that, idx+run, visitor);
                        visitor.close(run);
                    }
                    
                    break;
                case SequentialTrieParser.TYPE_VALUE_BYTES:
                    
                    //take all the bytes until we read the stop byte.
                    //is no body here so i + 2 is next
                    //open bytes
                    //visit
                    //close bytes
                    
                    break;
                case SequentialTrieParser.TYPE_VALUE_NUMERIC:
                    
                   
                    //take all the bytes that are ASCII numbers
                    //is no body here so i + 1 is next
                    //open int
                    //visit
                    //close int
                    
                    
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
        
        that.sourceBacking = source;
        that.sourcePos     = offset;
        that.sourceLen     = length;
        that.sourceMask    = mask;        
        
    }
    
    public static boolean parseHasContent(SequentialTrieParserReader reader) {
        return reader.sourceLen>0;
    }
    
    public static int parseNext(SequentialTrieParserReader reader, SequentialTrieParser trie) {
                
        return query(reader, trie, reader.sourceBacking, reader.sourcePos, reader.sourceLen, reader.sourceMask);
                
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

    
    public static int query(SequentialTrieParserReader reader, SequentialTrieParser trie, 
                            byte[] source, int localSourcePos, int sourceLength, int sourceMask) {
        
        reader.capturedPos = 0;
        reader.capturedBlobArray = source;

        reader.pos = 0;
        reader.runLength = 0;
        
        reader.hasSafePoint = false;
        reader.safeReturnValue = -1;
        reader.safeCapturedPos = -1;
        
        reader.altStackPos = 0;
          
        int type = trie.data[reader.pos++];
        top:
        while (type != SequentialTrieParser.TYPE_END) {
            
            if (type==SequentialTrieParser.TYPE_BRANCH_VALUE) {
                
                reader.pos = SequentialTrieParser.jumpOnBit((short) source[sourceMask & localSourcePos], reader.pos, trie.data);
                
            } else if (type == SequentialTrieParser.TYPE_RUN) {
                
                //run
                int run = trie.data[reader.pos++];
        
                if (SequentialTrieParser.skipDeepChecks && !reader.hasSafePoint && 0==reader.altStackPos) {
                    reader.pos += run;
                    localSourcePos += run;
                } else {
                    
                    int r = run;
                    while (--r >= 0) {
                        if (trie.data[reader.pos++] != source[sourceMask & localSourcePos++]) {

                            //always use the last safe point if one has been found.
                            if (reader.hasSafePoint) {     
                                localSourcePos = restoreSafePoint(reader);
                                return reader.safeReturnValue;
                            } else {
                                if (reader.altStackPos > 0) {
                                    //try other path
                                    //reset all the values to the other path and continue from the top
                                    localSourcePos = popAlt(reader);
                                    type = trie.data[reader.pos++];  
                                    continue top;
                                    
                                } else {
                                    throw new RuntimeException("check prev branch, no match at pos "+reader.pos+"  \n"+trie);
                                }
                            }
                                                        
                        }
                    }      
                    
                }
                reader.runLength += run;
                
            } else if (type == SequentialTrieParser.TYPE_SAFE_END) {                    
                
                recordSafePointEnd(reader, trie);                    
                if (sourceLength == reader.runLength) {
                    //hard stop passed in forces us to use the safe point
                    reader.sourceLen -= (localSourcePos-reader.sourcePos);
                    reader.sourcePos = localSourcePos;
                    return reader.safeReturnValue;
                }                    

            } else if (type == SequentialTrieParser.TYPE_VALUE_NUMERIC) {
                
                parseNumeric(reader,source,localSourcePos, sourceLength-reader.runLength, sourceMask, (int) trie.data[reader.pos++]);
            
            } else if (type == SequentialTrieParser.TYPE_VALUE_BYTES) {
            
                parseBytes(reader,source,localSourcePos, sourceLength-reader.runLength, sourceMask, trie.data[reader.pos++]);
            
            } else if (type == SequentialTrieParser.TYPE_ALT_BRANCH) {
      
                reader.pos = altBranch(reader, trie.data, localSourcePos);                                   
                
            } else  {
                System.out.println(trie);
                throw new UnsupportedOperationException("Bad jump length now at position "+(reader.pos-1)+" type found "+type);
            }
            
            type = trie.data[reader.pos++]; 
        }
        
        reader.sourceLen -= (localSourcePos-reader.sourcePos);
        reader.sourcePos = localSourcePos;
        
        return SequentialTrieParser.readEndValue(trie.data,reader.pos);
        
        
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
        localSourcePos     = reader.altStackA[reader.altStackPos];
        reader.capturedPos = reader.altStackB[reader.altStackPos];
        reader.pos         = reader.altStackC[reader.altStackPos--];
        return localSourcePos;
    }

    private static int altBranch(SequentialTrieParserReader reader, short[] data, int offset) {
        return altBranch(reader, reader.pos, offset, data[reader.pos++], data[reader.pos]);
    }

    private static int altBranch(SequentialTrieParserReader reader, int pos, int offset, int jump, int peekNextType) {
        if (SequentialTrieParser.TYPE_VALUE_BYTES == peekNextType || SequentialTrieParser.TYPE_VALUE_NUMERIC==peekNextType) {
            //Take the Jump value first, the local value has an extraction.
            //push the LocalValue
            pushAlt(reader, pos, offset);
            pos+=jump;            
        } else {
            //Take the Local value first
            //push the JumpValue
            pushAlt(reader, pos+jump, offset);
        }
        return pos;
    }

    private static void pushAlt(SequentialTrieParserReader reader, int pos, int offset) {
        
        reader.altStackA[reader.altStackPos] = offset;
        reader.altStackB[reader.altStackPos] = reader.capturedPos;
        reader.altStackC[reader.altStackPos] = pos;
        reader.altStackPos++;
        
    }

    private static void recordSafePointEnd(SequentialTrieParserReader reader, SequentialTrieParser trie) {
        reader.hasSafePoint = true;
        reader.safeReturnValue = SequentialTrieParser.readEndValue(trie.data,reader.pos);
        reader.safeCapturedPos = reader.capturedPos;
        
        reader.safeSourcePos = reader.sourcePos;
        
        
        reader.pos += SequentialTrieParser.SIZE_OF_RESULT;
        //if the following does not match we will return this safe value.
        //we do not yet have enough info to decide if this is the end or not.
    }

    
    private static int parseBytes(SequentialTrieParserReader reader, byte[] source, int sourcePos, int sourceLengh, int sourceMask, int stopValue) {
                
        int len = 0;
        int bytesPos = sourcePos;
        
        short c = 0;
        int limit = sourcePos + sourceLengh;
        do {
            c = source[sourceMask & sourcePos++];                    
            if (stopValue != c) {
                len++;
                continue;
            } else {
                break;
            }
        }  while (sourcePos < limit);
        
        
        reader.capturedValues[reader.capturedPos++] = 0;  //this is not a number but instead bytes
        reader.capturedValues[reader.capturedPos++] = bytesPos;
        reader.capturedValues[reader.capturedPos++] = len;
        reader.capturedValues[reader.capturedPos++] = sourceMask;
        
        return sourcePos;
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
    
    public static void capturedFieldBytes(SequentialTrieParserReader reader, int idx, DataOutput out) {
   
        int pos = idx*4;
        
        int type = reader.capturedValues[pos++];
        assert(type==0);
        int p = reader.capturedValues[pos++];
        int l = reader.capturedValues[pos++];
        int m = reader.capturedValues[pos++];

        byte[] array = reader.capturedBlobArray;        
        try {
            while (--l >= 0) {
                    out.write(array[m & p++]);
            }
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        
    }
   
    public static void writeCapturedValuesToPipe(SequentialTrieParserReader reader, Pipe<?> target) {
         
        int limit = reader.capturedPos;
        
        int i = 0;
        while (i < limit) {
            
            int type = reader.capturedValues[i++];
            
            if (0==type) {
                int p = reader.capturedValues[i++];
                int l = reader.capturedValues[i++];
                int m = reader.capturedValues[i++];                
                target.addByteArrayWithMask(target, m, l, reader.capturedBlobArray, p);
            } else {
                
                target.addIntValue(type, target);
                target.addIntValue(reader.capturedValues[i++], target);
                target.addIntValue(reader.capturedValues[i++], target);
                target.addIntValue(reader.capturedValues[i++], target);
                
            }
            
        }
        
    }
    
    
    

}
