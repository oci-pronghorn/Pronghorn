package com.ociweb.pronghorn.util;

public class ByteSequenceReader {

    final int maxValues = 16;
    
    int[] pos = new int[maxValues];
    int posIdx = 0;
    
    int length = 0;
    int value = 0;

    private byte numericSign;
    private long numericValue;
    private byte numericLength;
    private byte numericBase;

    private int bytesPos;
    private int bytesLen;
    
    
    public ByteSequenceReader() {
        
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
    public void visit(ByteSequenceMap that, int i, ByteSquenceVisitor visitor) {
        
            switch (that.data[i]) {
                case ByteSequenceMap.TYPE_BRANCH_VALUE:
                case ByteSequenceMap.TYPE_BRANCH_LENGTH:
                    
                    int localJump = i + ByteSequenceMap.SIZE_OF_BRANCH;
                    int farJump   = i + ((that.data[i+2]<<16) | (0xFFFF&that.data[i+3])); 
                    
                    visit(that, localJump, visitor);
                    visit(that, farJump, visitor);
                    
                    break;
                case ByteSequenceMap.TYPE_RUN:
                    
                    final int run = that.data[i+1];
                    final int idx = i + ByteSequenceMap.SIZE_OF_RUN;
                    
                    if (visitor.open(that.data, idx, run)) {
                    
                        visit(that, idx+run, visitor);
                        visitor.close(run);
                    }
                    
                    break;
                case ByteSequenceMap.TYPE_VALUE_BYTES:
                    
                    //take all the bytes until we read the stop byte.
                    //is no body here so i + 2 is next
                    //open bytes
                    //visit
                    //close bytes
                    
                    break;
                case ByteSequenceMap.TYPE_VALUE_NUMERIC:
                    
                   
                    //take all the bytes that are ASCII numbers
                    //is no body here so i + 1 is next
                    //open int
                    //visit
                    //close int
                    
                    
                    break;
                case ByteSequenceMap.TYPE_END:
                    
                    visitor.end(
                                ((that.data[i+1]<<16) | (0xFFFF&that.data[i+2]))
                               );
                    
                    break;
                default:
                    throw new UnsupportedOperationException("ERROR Unrecognized value\n");
            }            
        
        
        
    }
    
    public int query(ByteSequenceMap that, 
                     byte[] source, int offset, int length, int mask) {
        
        int pos = 0;
        short[] data = that.data;
        int runLength = 0;
        int sum = 0;
        
        //TODO: store values in stack
        //TODO: store text as pos & length in source?
        
        //TODO: need new SAFE point TYPE: keep where we are and return last safe point upon branched failure.
        
        
        int type = data[pos++];
        while (type != ByteSequenceMap.TYPE_END) {
        
        
            
            if (type==ByteSequenceMap.TYPE_BRANCH_VALUE) {
                //TODO: urgent split on bit.
                pos = ByteSequenceMap.jumpEQ(pos, data, (short) source[mask & offset]);
                
                //TODO: new default jump
                //      store pos of jump and choice
                //      if run does not match check other side (requires run check)
                
                
            } else if (type == ByteSequenceMap.TYPE_RUN) {
                //run
                int run = data[pos++];
        
                if (ByteSequenceMap.skipDeepChecks) {
                    pos += run;
                    offset += run;
                } else {
                    
                    int r = run;
                    while (--r >= 0) {
                        if (data[pos++] != source[mask & offset++]) {
                            throw new RuntimeException("check prev branch, no match at pos "+pos+"  \n"+that);
                        }
                    }                        
                }
                runLength+=run;
                
            } else {
                if (type == ByteSequenceMap.TYPE_BRANCH_LENGTH) {
                    pos = ByteSequenceMap.jumpNEQ(pos, data, (short) length); ////TODO: source length can not be used since it will will go to the end of the file not route.
                } else {
                    if (type == ByteSequenceMap.TYPE_VALUE_NUMERIC) {
                        parseNumeric(this,source,offset, length-runLength, mask, data[pos++]);
                    } else if (type == ByteSequenceMap.TYPE_VALUE_BYTES) {
                        parseBytes(this,source,offset, length-runLength, mask, data[pos++]);
                    } else {
                        
                        System.out.println(that);
                        throw new UnsupportedOperationException("Bad jump length now at position "+(pos-1)+" type found "+type);
                        
                    }
                }
            } 
            
            type = data[pos++]; 
        }
        
        //end
        
        length = runLength;                  
        
        // return ((0xFFFF & data[pos++])<<16) | ((0xFFFF & data[pos++]));
         
        value = runLength<=length ? 
                ByteSequenceMap.readEndValue(data,pos) : 
                Integer.MIN_VALUE;
               
        return value; 
        
        
    }

    
    private static int parseBytes(ByteSequenceReader reader, byte[] source, int sourcePos, int sourceLenght, int sourceMask, int stopValue) {
        
        int len = 0;
        int bytesPos = sourcePos;
        
        short c = 0;
        do {
            c = source[sourceMask & sourcePos++];                    
            if (stopValue != c) {
                len++;
                continue;
            } else {
                break;
            }
        }  while (true);
        
        reader.bytesLen = len;
        reader.bytesPos = bytesPos;
        
        return sourcePos;
    }
    
    private static int parseNumeric(ByteSequenceReader reader, byte[] source, int sourcePos, int sourceLenght, int sourceMask, int numType) {
        
        byte sign = 1;
        long intValue = 0;
        byte intLength = 0;
        byte base=10;

        //NOTE: these Numeric Flags are invariants consuming runtime resources, this tree could be pre-compiled to remove them if neded.
        if (0!=(ByteSequenceMap.NUMERIC_FLAG_SIGN&numType)) {
            final short c = source[sourceMask & sourcePos];
            if (c=='-') { //NOTE: check ASCII table there may be a fater way to do this.
                sign = -1;
            }
            if (c=='+') {
                sourcePos++;
            }
        }

        if (sourceLenght > 20) { //TODO: change to nonbranching.
            sourceLenght = 20;
        }
        
        if (0==(ByteSequenceMap.NUMERIC_FLAG_HEX&numType) | ('0'!=source[sourceMask & sourcePos+1])| ('x'!=source[sourceMask & sourcePos+2])  ) {                            
            base = 10;
            short c = 0;
            do {
                c = source[sourceMask & sourcePos++];                    
                if ((c>='0') && (c<='9') && intLength<sourceLenght) {
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
                
                if ((c>='0') && (c<='9') && intLength<sourceLenght) {
                    intValue = (intValue<<4)+(c-'0');
                    intLength++;
                    continue;
                } else  if ((c>='a') && (c<='f') && intLength<sourceLenght) {
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
        
        
        //assign all values back
        reader.numericSign = sign;
        reader.numericValue = intValue;
        reader.numericLength = intLength;
        reader.numericBase = base;

        return sourcePos;
    }
    

}
