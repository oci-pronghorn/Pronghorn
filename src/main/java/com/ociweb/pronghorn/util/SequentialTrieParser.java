package com.ociweb.pronghorn.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Optimized for fast lookup and secondarily size. 
 * Inserts may require data copy and this could be optimized in future releases if needed.
 * 
 * @author Nathan Tippy
 *
 */
public class SequentialTrieParser {
    
    //%bX - where X is the excluded stop byte
    //%i  - signed int decimal
    //%u  - unsigned int decimal
    //%f  - fixed place (finance)
    // 123  -123 +123  23.23 , hex and base 10? bult in support for 0x prefx?
    //NOTE: real fraction could be supported by sending both number and denominator
    
    
    private static final Logger logger = LoggerFactory.getLogger(SequentialTrieParser.class);

    static final byte TYPE_SAFE_END            = 0X00; 
    static final byte TYPE_BRANCH_VALUE        = 0x01; //followed by mask & 2 short jump  
    static final byte TYPE_RUN                 = 0x03; //followed by length
    
    static final byte TYPE_VALUE_NUMERIC       = 0x04; //followed by type, parse right kind of number
    static final byte TYPE_VALUE_BYTES         = 0x05; //followed by stop byte, take all until stop byte encountered (AKA Wild Card)
    
    static final byte TYPE_VALUE_COND_NUMERIC  = 0x06; //followed by trigger value and kind of number
    
    ///TODO: new numeric type, if short X '.' or '/' THEN parse number ELSE  default short
    
    static final byte TYPE_END                 = 0x07; //followed by 4 bytes
    
    static final int SIZE_OF_RESULT               = 1;//2;//TODO: make values 1 short
        
    static final int SIZE_OF_BRANCH               = 1+1+1;
    static final int SIZE_OF_RUN                  = 1+1;
    static final int SIZE_OF_END                  = 1+SIZE_OF_RESULT;
    static final int SIZE_OF_VALUE_NUMERIC        = 1+1;
    static final int SIZE_OF_VALUE_COND_NUMERIC   = 1+2;
    static final int SIZE_OF_SAFE_END             = 1+SIZE_OF_RESULT;//Same as end except we keep going and store this
    static final int SIZE_OF_VALUE_BYTES          = 1+1;
    
    static final boolean skipDeepChecks = true;//these runs are not significant and do not provide any consumed data.
    //humans require long readable URLs but the machine can split them into categories on just a few key bytes
    
    
    //numeric type bits:
    //   leading sign (only in front)
    static final byte NUMERIC_FLAG_SIGN  =  1;
    //   hex values can start with 0x, hex is all lower case abcdef
    static final byte NUMERIC_FLAG_HEX   =  2;
    
    
    final short[] data;
    private int limit = 0;
    
    public SequentialTrieParser(int size) {
        this.data = new short[size];
    }
    
    
    public int getLimit() {
        return limit;
    }
    
    public void setValue(byte[] source, int offset, int length, int mask, int value) {
        setValue(0, data, source, offset, length, mask, value);  
        
    }
    
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public StringBuilder toString(StringBuilder builder) {
        int i = 0;
        while (i<limit) {
            switch (data[i]) {
                case TYPE_SAFE_END:
                    i = toStringSafe(builder, i);
                    break;
                case TYPE_BRANCH_VALUE:
                    i = toStringBranchValue(builder, i);
                    break;
                case TYPE_VALUE_NUMERIC:
                    i = toStringNumeric(builder, i);
                    break;
                case TYPE_VALUE_BYTES:
                    i = toStringBytes(builder, i);
                    break;
                case TYPE_RUN:
                    i = toStringRun(builder, i);  
                    break;
                case TYPE_END:
                    i = toStringEnd(builder, i);
                    break;
                default:
                    return builder.append("ERROR Unrecognized value, remaining "+(limit-i)+"\n");
            }            
        }
        return builder;
    }
    
    private int toStringSafe(StringBuilder builder, int i) {
        builder.append("SAFE");
        builder.append(data[i]).append("[").append(i++).append("], ");
        if (SIZE_OF_RESULT>1) {
            builder.append(data[i]).append("[").append(i++).append("], ");
        }
        builder.append(data[i]).append("[").append(i++).append("], \n");
        return i;
    }

    private int toStringNumeric(StringBuilder builder, int i) {
        builder.append("Int");
        builder.append(data[i]).append("[").append(i++).append("], ");
        
        builder.append(data[i]).append("[").append(i++).append("], \n");
        return i;
        
    }
    
    private int toStringBytes(StringBuilder builder, int i) {
        builder.append("Bytes");
        builder.append(data[i]).append("[").append(i++).append("], ");
        
        builder.append(data[i]).append("[").append(i++).append("], \n");
        return i;
    }
    
    
    private int toStringEnd(StringBuilder builder, int i) {
        builder.append("END");
        builder.append(data[i]).append("[").append(i++).append("], ");
        if (SIZE_OF_RESULT>1) {
            builder.append(data[i]).append("[").append(i++).append("], ");
        }
        builder.append(data[i]).append("[").append(i++).append("], \n");
        return i;
    }


    private int toStringRun(StringBuilder builder, int i) {
        builder.append("RUN");
        builder.append(data[i]).append("[").append(i++).append("], ");
        int len = data[i];
        builder.append(data[i]).append("[").append(i++).append("], ");
        while (--len >= 0) {
            builder.append(data[i]).append("[").append(i++).append("], ");
        }
        builder.append("\n");
        return i;
    }


    private int toStringBranchValue(StringBuilder builder, int i) {
        builder.append("BRANCH_VALUE");
        builder.append(data[i]).append("[").append(i++).append("], "); //TYPE
        
        builder.append(data[i]).append("[").append(i++).append("], "); //MASK FOR CHAR
        
       // builder.append(data[i]).append("[").append(i++).append("], "); //JUMP
        builder.append(data[i]).append("[").append(i++).append("], \n");//JUMP
        return i;
    }

    
    static int jumpOnBit(int pos, short[] data, short source) {                
        short critera  = data[pos++];//high 8 is left or right, low 8 is the magic bit to mask
        int jumpBit = ((source & (0xFF&critera))-1)>>>8;
        return (((short)0xFFFF&(~jumpBit ^ critera>>>8)) & (int) data[pos++]) + pos;
    }

    private void setValue(int pos, short[] data, byte[] source, int sourcePos, int sourceLength, int sourceMask, int value) {
        
        assert(value >= 0);
        assert(value <= 0x7FFF_FFFF); 
        
//        boolean noVarLengthContent = true; //set to false if 'value int' or 'value bytes' is encountered

        if (0!=limit) {
            int length = 0;
                    
            while (true) {
            
                int type = 0xFF & data[pos++];
                switch(type) {
                    case TYPE_BRANCH_VALUE:
                        pos = jumpOnBit(pos, data, (short) source[sourceMask & sourcePos]);
                        break;
                    case TYPE_VALUE_NUMERIC:                        
                        sourcePos = stepOverNumeric(source, sourcePos, sourceMask, (int) data[pos++]);
                        break;
                    case TYPE_VALUE_BYTES:
                        sourcePos = stepOverBytes(source, sourcePos, sourceMask, data[pos++]);
                        break;
                    case TYPE_RUN:
                        //run
                        int runPos = pos++;
                        int run = data[runPos];
                              
                        if (sourceLength < run+length) {
                            
                            int r = sourceLength-length;
                            if (r>=run) {
                                throw new UnsupportedOperationException();
                            }
                            int afterWhileLength = length+r;
                            int afterWhileRun    = run-r;
                            while (--r >= 0) {
                                if (data[pos++] != source[sourceMask & sourcePos++]) {
                          //          String before = validateTree(type);
                          //          System.err.println("before:"+before);
                                    insertAtBranchValue(pos, data, source, sourceLength, sourceMask, value, length, runPos, run, r+afterWhileRun, sourcePos-1);
                        //            System.err.println("insert branch at "+pos);
                       //             validateTree(type);
                                    return;
                                }
                            }
                            length = afterWhileLength;
                            //matched up to this point but this was shorter than the run so insert a safe point
                            insertNewSafePoint(pos, data, source, sourcePos, afterWhileRun, sourceMask, value, runPos);     
                            return;
                        }                        
                        
                        int r = run;
                        while (--r >= 0) {
                            if (data[pos++] != source[sourceMask & sourcePos++]) {
                                insertAtBranchValue(pos, data, source, sourceLength, sourceMask, value, length, runPos, run, r, sourcePos-1);
                  //              validateTree(type);
                                return;
                            }
                        }
                        
                        length+=run;

                        break;
                    case TYPE_END:
                        
                        if (sourceLength>length) {
                            convertEndToNewSafePoint(pos, data, source, sourcePos, sourceLength-length, sourceMask, value);  
                    //        validateTree(type);
                        } else {
                            writeEndValue(data, pos, value);
                       //     validateTree(type);
                        }
                        return;
                        
                        
                    case TYPE_SAFE_END:
                        if (sourceLength>length) {
                            ///jump over the safe end values and continue on
                            pos += SIZE_OF_RESULT;
                            break;                            
                        } else {
                            pos = writeEndValue(data, pos, value);
                    //        validateTree(type);
                            return;
                        }
                    default:
                        System.out.println(this);
                        throw new UnsupportedOperationException("unknown op "+type);
                }
                
             //   validateTree(type);                

            }
        } else {
            pos = writeRuns(data, pos, source, sourcePos, sourceLength, sourceMask);
            limit = writeEnd(data, pos, value);
        }
    }


//    private String validateTree(int type) {
//        String temp = this.toString();
//        if (temp.contains("ERROR")) {
//            System.err.println(temp);
//            new Exception("failure on last type "+type).printStackTrace();;
//            System.exit(0);
//        }
//        return temp;
//    }


    private void convertEndToNewSafePoint(int pos, short[] data, byte[] source, int sourcePos, int sourceLength, int sourceMask, int value) {
        //convert end to safe
        
        if (data[pos-1] != TYPE_END) {
            throw new UnsupportedOperationException();
        }
        data[--pos] = TYPE_SAFE_END; //change to a safe
        pos += SIZE_OF_SAFE_END;

        //now insert the needed run
        makeRoomForInsert(sourceLength, data, pos, SIZE_OF_END + sourceLength);        
        pos = writeRuns(data, pos, source, sourcePos, sourceLength, sourceMask);        
        pos = writeEnd(data, pos, value);
    }

    private int insertNewSafePoint(int pos, short[] data, byte[] source, int sourcePos, int sourceLength, int sourceMask, int value, int runLenPos) {
        //convert end to safe
        
        makeRoomForInsert(sourceLength, data, pos, SIZE_OF_SAFE_END);
        
        data[pos++] = TYPE_SAFE_END;
        pos = writeEndValue(data, pos, value);
        pos = writeRunHeader(data, pos, sourceLength);
        data[runLenPos] -= sourceLength;//previous run is shortened buy the length of this new run
        return pos;
    }
    

    private void insertAtBranchValue(int pos, short[] data, byte[] source, int sourceLength, int sourceMask, int value, int length, int runPos, int run, int r1, int sourceCharPos) {
        r1++;//remaining
        if (r1 == run) {
            r1 = 0; //keep entire run and do not split it.
            insertAtBranchValue(r1, data, pos-3, source, sourceCharPos, sourceLength-length, sourceMask, value);
        } else {
            int dataPos = pos-1;                                       
            data[runPos] = (short)(dataPos-runPos-1);
           
          //  System.out.println("some matched but remaining "+r1+" did not new run is  "+data[runPos]+" remaining length is "+((sourceLength-(length+data[runPos]))));
           
            
            int computedRemainingLength = sourceLength-(length+data[runPos]);
            if (computedRemainingLength<0) {
                throw new UnsupportedOperationException("len:"+computedRemainingLength+"  srcLen:"+sourceLength+" len1:"+length+" posRun:"+data[runPos]);
            }
         //   System.err.println("rem: "+computedRemainingLength+" vs "+r1);
            
            insertAtBranchValue(r1, data, dataPos, source, sourceCharPos, computedRemainingLength , sourceMask, value);
        }
    }


    private int stepOverBytes(byte[] source, int sourcePos, int sourceMask, final short stop) {
        short t = 0;
        do {
            t = source[sourceMask & sourcePos++];
        }  while (stop!=t);
        return sourcePos;
    }


    private int stepOverNumeric(byte[] source, int sourcePos, int sourceMask, int numType) {

        //NOTE: these Numeric Flags are invariants consuming runtime resources, this tree could be pre-compiled to remove them if neded.
        if (0!=(NUMERIC_FLAG_SIGN&numType)) {
            final short c = source[sourceMask & sourcePos];
            if (c=='-' || c=='+') {
                sourcePos++;
            }                         
        }
                         
        if (0==(NUMERIC_FLAG_HEX&numType) | ('0'!=source[sourceMask & sourcePos+1])| ('x'!=source[sourceMask & sourcePos+2])  ) {                            
            short c = 0;
            do {
                c = source[sourceMask & sourcePos++];
            }  while ((c>='0') && (c<='9'));
        } else {
            sourcePos+=2;//skipping over the 0x checked above
            short c = 0;
            do {
                c = source[sourceMask & sourcePos++];
            }  while (((c>='0') && (c<='9')) | ((c>='a') && (c<='f'))  );
        }

        return sourcePos;
    }


    private void insertAtBranchValue(int danglingByteCount, short[] data, int pos, byte[] source, int sourcePos,final int sourceLength, int sourceMask, int value) {
        
        if (sourceLength > 0x7FFF || sourceLength < 1) {
            throw new UnsupportedOperationException("does not support strings beyond this length "+0x7FFF+" value was "+sourceLength);
        }
        
        final int requiredRoom = SIZE_OF_END + SIZE_OF_BRANCH +  (((~(sourceLength-1))>>31)&SIZE_OF_RUN)+ sourceLength;
        
        int oldValueIdx = makeRoomForInsert(danglingByteCount, data, pos, requiredRoom);

        short a = source[sourcePos & sourceMask];
        short b = data[oldValueIdx];
        short jumpData = findSingleBitMask(a, b);
        
        pos = writeBranch(TYPE_BRANCH_VALUE, data, pos, requiredRoom, jumpData);
        pos = writeRuns(data, pos, source, sourcePos, sourceLength, sourceMask);

        writeEnd(data, pos, value);
        
    }


    private short findSingleBitMask(short a, short b) {
        int mask = 1<<5; //default of sign bit, only used when nothing replaces it.
        
        int i = 8; 
        while (--i>=0) {            
            if (5!=i) { //sign bit, we do not use it unless all the others are tested first                
                mask = 1 << i;
                if ((mask&a) != (mask&b)) {
                    break;
                }
            }          
        }        
        return (short)(( 0xFF00&((mask&b)-1) ) | mask); //high byte is on when A matches mask
    }

    private int makeRoomForInsert(int danglingByteCount, short[] data, int pos, int requiredRoom) {
                
        int len = limit-pos;
        if (danglingByteCount>0) {
            requiredRoom+=SIZE_OF_RUN; //added because we will prepend this with a TYPE_RUN header to close the dangling bytes
        }
        limit+=requiredRoom;      
        
        if (len<0) {
            return pos;//nothing to be moved
        }
                
        updatePreviousJumpDistances(0, data, pos, requiredRoom);        
        
        int newPos = pos+requiredRoom;
        
        System.arraycopy(data, pos, data, newPos, len);
        
        if (danglingByteCount>0) {//do the prepend because we now have room
            data[newPos-2] = TYPE_RUN;
            data[newPos-1] = (short)danglingByteCount;
        } else {
            //new position already has the start of run so move cursor up to the first data point 
            newPos+=SIZE_OF_RUN;
        }
        return newPos;
    }


    private void updatePreviousJumpDistances(int i, short[] data, int limit, int requiredRoom) {

       // System.out.println("Xxxx  update previous by "+requiredRoom+" that jump to values on or after "+limit);
        
        while (i<limit) {
            switch (data[i]) {
                case TYPE_SAFE_END:
                    i += SIZE_OF_SAFE_END;
                    break;
                case TYPE_BRANCH_VALUE:
                                      
                    int jmp = data[i+2];//(((int)data[i+2]) << 16)|(0xFFFF&data[i+3]);
                    int newPos = i+jmp;
                    if (newPos >= limit) {
                        //adjust this value because it jumps over the new inserted block
                        jmp += requiredRoom; 
                        
                        if (jmp > 0x7FFF) {
                            throw new UnsupportedOperationException("This content is too large, use shorter content or modify this code to make multiple jumps.");
                        }
                                                
                        data[i+2] = (short)(0xFFFF&(jmp));
                        
                    }
                    i += SIZE_OF_BRANCH;
                    break;                    
                case TYPE_VALUE_NUMERIC:
                    i += SIZE_OF_VALUE_NUMERIC;
                    break;
                case TYPE_VALUE_BYTES:
                    i += SIZE_OF_VALUE_BYTES;
                    break;                    
                case TYPE_RUN:
                    i = i+SIZE_OF_RUN+data[i+1];
                    break;
                case TYPE_END:
                    i += SIZE_OF_END;
                    break;
                default:
                    System.out.println(this);
                    throw new UnsupportedOperationException("ERROR Unrecognized value "+data[i]+" at "+i);
            }            
        }
    }


    private int writeBranch(byte type, short[] data, int pos, int requiredRoom, short criteria) {
        
        if (requiredRoom > 0x7FFF) {
            throw new UnsupportedOperationException("This content is too large, use shorter content or modify this code to make multiple jumps.");
        }
        
       // System.err.println("wrote type "+type+" into "+pos);
        
        requiredRoom -= SIZE_OF_BRANCH;//subtract the size of the branch operator
        data[pos++] = type;
        data[pos++] = criteria;
        
        data[pos++] = (short)(0xFFFF&requiredRoom);
        
        return pos;
    }


    private int writeEnd(short[] data, int pos, int value) {
        data[pos++] = TYPE_END;
        return writeEndValue(data, pos, value);
    }


    private int writeEndValue(short[] data, int pos, int value) {
        if (1==SIZE_OF_RESULT) {
            data[pos++] = (short)(0xFFFF&value);
        } else {
            data[pos++] = (short)(0xFFFF&(value>>16));
            data[pos++] = (short)(0xFFFF&value);
        }
        return pos;
    }
    
    static int readEndValue(short[] data, int pos) {
        if (1==SIZE_OF_RESULT) {
            return 0xFFFF & data[pos];
        } else {
            return ((0xFFFF & data[pos++])<<16) | ((0xFFFF & data[pos++]));
        }
    }
 
    private int writeRuns(short[] data, int pos, byte[] source, int sourcePos, int sourceLength, int sourceMask) {
        pos = writeRunHeader(data, pos, sourceLength);
        int runLeft = sourceLength;
        while (--runLeft >= 0) {
                data[pos++] = source[sourceMask & sourcePos++];
       }
       return pos;
    }


    private int writeRunHeader(short[] data, int pos, int sourceLength) {
        
        if (sourceLength > 0x7FFF || sourceLength < 1) {
            throw new UnsupportedOperationException("does not support strings beyond this length "+0x7FFF+" value was "+sourceLength);
        }
        
        data[pos++] = TYPE_RUN;
        data[pos++] = (short)sourceLength;
        return pos;
    }

    
}
