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
public class ByteSequenceMap {
    
    private static final Logger logger = LoggerFactory.getLogger(ByteSequenceMap.class);

    static final byte TYPE_BRANCH_VALUE    = 0x01; //followed by mask & 2 short jump  
    static final byte TYPE_BRANCH_LENGTH   = 0x02; //followed by mask & 2 short jump
    static final byte TYPE_RUN             = 0x03; //followed by length
    
    static final byte TYPE_VALUE_INT       = 0x04; //take all ascii ints until end or not int incountered
    static final byte TYPE_VALUE_BYTES     = 0x05; //followed by stop byte, take all until stop byte encountered (AKA Wild Card)

    static final byte TYPE_END             = 0x07; //followed by 4 bytes
    
    
    static final int SIZE_OF_BRANCH      = 1+1+1;//2; //TODO: make branches 1 short only, now in testing may root this change back.
    static final int SIZE_OF_RUN         = 1+1;
    static final int SIZE_OF_END         = 1+2;
    static final int SIZE_OF_VALUE_INT   = 1;
    static final int SIZE_OF_VALUE_BYTES = 1+1;
    
    static final boolean skipDeepChecks = true;//these runs are not significant and do not provide any consumed data.
    //humans require long readable URLs but the machine can split them into categories on just a few key bytes
    
    
    private final int size;
    final short[] data;
    private int limit = 0;
    
    public ByteSequenceMap(int size) {
        this.size = size;
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
                case TYPE_BRANCH_VALUE:
                    i = toStringBranchValue(builder, i);
                    break;
                case TYPE_BRANCH_LENGTH:
                    i = toStringBranchLength(builder, i);
                    break;
                case TYPE_VALUE_INT:
                    i = toStringInt(builder, i);
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
                    return builder.append("ERROR Unrecognized value\n");
            }            
        }
        return builder;
    }

    private int toStringInt(StringBuilder builder, int i) {
        builder.append("Int");
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
        
        builder.append(data[i]).append("[").append(i++).append("], ");
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
    
    private int toStringBranchLength(StringBuilder builder, int i) {
        builder.append("BRANCH_LENGTH");
        builder.append(data[i]).append("[").append(i++).append("], "); //TYPE
        
        builder.append(data[i]).append("[").append(i++).append("], "); //MASK FOR LENGTH
        
       // builder.append(data[i]).append("[").append(i++).append("], "); //JUMP
        builder.append(data[i]).append("[").append(i++).append("], \n");//JUMP
        return i;
    }
    
    static int jumpEQ(int pos, short[] data, short branchOn) {
        //NOTE: negative values are causing a problem. so we mask to FFFF
        return (( ( ( (0xFFFF)& (branchOn^data[pos++])))  -1)>>31 & ((((int)data[pos++]) /*<< 16*/) /*|(0xFFFF&data[pos++])*/)) + pos;
    }
    
    static int jumpNEQ(int pos, short[] data, short branchOn) {
         //NOTE: negative values are causing a problem. so we mask to FFFF
        return ((~((( (0xFFFF)& (branchOn^data[pos++])))  -1))>>31 & ((((int)data[pos++]) /*<< 16*/)/*|(0xFFFF&data[pos++])*/)) + pos;
    }
    

    private void setValue(int pos, short[] data, byte[] source, int sourcePos, int sourceLength, int sourceMask, int value) {
        
        assert(value >= 0);
        assert(value <= 0x7FFF_FFFF); 
        
        boolean noVarLengthContent = true; //set to false if 'value int' or 'value bytes' is encountered

        if (0!=limit) {
            int length = 0;
                    
            while (true) {
            
                int type = 0xFF & data[pos++];
                switch(type) {
                    case TYPE_BRANCH_VALUE:
                        pos = jumpEQ(pos, data, (short) source[sourceMask & sourcePos]);
                        break;
                    case TYPE_BRANCH_LENGTH:
                        pos = jumpNEQ(pos, data, (short)sourceLength);
                        break;
                    case TYPE_VALUE_INT:
                        short c = 0;
                        do {
                            c = source[sourceMask & sourcePos++];
                        }  while ((c>='0') && (c<='9'));
                        
                        break;
                    case TYPE_VALUE_BYTES:
                        final short stop = data[pos++];
                        short t = 0;
                        do {
                            t = source[sourceMask & sourcePos++];
                        }  while (stop!=t);
                        break;
                    case TYPE_RUN:
                        //run
                        int runPos = pos++;
                        int run = data[runPos];
                              
                        if (noVarLengthContent && (sourceLength < run+length)) {
                            //branch on length, since we know this length is shorter than the run
                            //this is faster and provides support for identical values where one of a different length                            
                            insertAtBranchLength(0, data, pos-2, source, sourcePos, sourceLength-length, sourceMask, value, length, sourceLength);

                            return;
                        }
                        
                        
                        int r = run;
                        while (--r >= 0) {
                            if (data[pos++] != source[sourceMask & sourcePos++]) {
                                int r1 = r;
                                int sourceCharPos = sourcePos-1;
                                            
                                
                                r1++;//remaining
                                if (r1 == run) {
                                    r1 = 0; //keep entire run and do not split it.
                                    insertAtBranchValue(r1, data, pos-3, source, sourceCharPos, sourceLength-length, sourceMask, value);
                                } else {
                                    int dataPos = pos-1;                                       
                                    data[runPos] = (short)(dataPos-runPos-1);
                                   // System.out.println("some matched but remaining "+r+" did not new run is  "+data[runPos]+" remaining length is "+((sourceLength-(length+data[runPos]))));
                                    insertAtBranchValue(r1, data, dataPos, source, sourceCharPos, (sourceLength-(length+data[runPos])) , sourceMask, value);
                                }
                                return;
                            }
                        }
                        
                        length+=run;

                        break;
                    case TYPE_END:
                        
                        if (sourceLength>length) {
                            //if we still have data left add a branch instead of changing the value                            
                            insertAtBranchLength(0, data, pos-1, source, sourcePos, sourceLength-length, sourceMask, value, length, sourceLength);
                            return;
                            
                        } else {
                            pos = writeEndValue(data, pos, value);
                        }
                        return;
                    default:
                        System.out.println(this);
                        throw new UnsupportedOperationException("unknown op "+type);
                }
                
                

            }
        } else {
            pos = writeRuns(data, pos, source, sourcePos, sourceLength, sourceMask);
            limit = writeEnd(data, pos, value);
        }
    }


    private void insertAtBranchValue(int danglingByteCount, short[] data, int pos, byte[] source, int sourcePos, int sourceLength, int sourceMask, int value) {
        
        final int requiredRoom = SIZE_OF_END + SIZE_OF_BRANCH +  (((~(sourceLength-1))>>31)&SIZE_OF_RUN)+ sourceLength;
        
        int oldValueIdx = makeRoomForInsert(danglingByteCount, data, pos, requiredRoom);

        pos = writeBranch(TYPE_BRANCH_VALUE, data, pos, requiredRoom, /*source[sourcePos & sourceMask]);//*/ data[oldValueIdx]);
        pos = writeRuns(data, pos, source, sourcePos, sourceLength, sourceMask);

        writeEnd(data, pos, value);
        
    }
    
    private void insertAtBranchLength(int danglingByteCount, short[] data, int pos, byte[] source, int sourcePos, int sourceLength, int sourceMask, int value, int runningLength, int totalLength) {
        
        final int requiredRoom = SIZE_OF_END + SIZE_OF_BRANCH +   (((~(sourceLength-1))>>31)&SIZE_OF_RUN)+ sourceLength;
        
        makeRoomForInsert(danglingByteCount, data, pos, requiredRoom);
        
        //System.err.println("write len branch "+runningLength+" new insert length of "+totalLength);
        
        pos = writeBranch(TYPE_BRANCH_LENGTH, data, pos, requiredRoom, (short)totalLength);//can not use runningLength, is often zero when splittin at begginning);
        pos = writeRuns(data, pos, source, sourcePos, sourceLength, sourceMask);
        writeEnd(data, pos, value);
        
    }


    private int makeRoomForInsert(int danglingByteCount, short[] data, int pos, int requiredRoom) {
                
        int len = limit-pos;
        
        if (danglingByteCount>0) {
            requiredRoom+=SIZE_OF_RUN; //added because we will prepend this with a TYPE_RUN header to close the dangling bytes
        }
        limit+=requiredRoom;
        
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
                case TYPE_BRANCH_VALUE:
                case TYPE_BRANCH_LENGTH:
                                      
                    int jmp = data[i+2];//(((int)data[i+2]) << 16)|(0xFFFF&data[i+3]);
                    int newPos = i+jmp;
                    if (newPos >= limit) {
                        //adjust this value because it jumps over the new inserted block
                        jmp += requiredRoom; 
                        
                        if (jmp > 0x7FFF) {
                            throw new UnsupportedOperationException("This content is too large, use shorter content or modify this code to make multiple jumps.");
                        }
                                                
                        data[i+2] = (short)(0xFFFF&(jmp/*>>16*/));
                      //  data[i+3] = (short)(0xFFFF&jmp);
                    }
                    i += SIZE_OF_BRANCH;
                    break;                    
                case TYPE_VALUE_INT:
                    i += SIZE_OF_VALUE_INT;
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


    private int writeBranch(byte type, short[] data, int pos, int requiredRoom, short comparison) {
        
        if (requiredRoom > 0x7FFF) {
            throw new UnsupportedOperationException("This content is too large, use shorter content or modify this code to make multiple jumps.");
        }
        
        requiredRoom -= SIZE_OF_BRANCH;//subtract the size of the branch operator
        data[pos++] = type;
        data[pos++] = comparison;
        
       // data[pos++] = (short)(0xFFFF&(requiredRoom>>16));
        data[pos++] = (short)(0xFFFF&requiredRoom);
        
        return pos;
    }


    private int writeEnd(short[] data, int pos, int value) {
        data[pos++] = TYPE_END;
        return writeEndValue(data, pos, value);
    }


    private int writeEndValue(short[] data, int pos, int value) {
       // data[pos++] = (short)(0xFFFF&value);
        data[pos++] = (short)(0xFFFF&(value>>16));
        data[pos++] = (short)(0xFFFF&value);
        return pos;
    }
    
    static int readEndValue(short[] data, int pos) {
        //return data[pos];
        return ((0xFFFF & data[pos++])<<16) | ((0xFFFF & data[pos++]));
        
    }
    
    //TODO: review how to merge jumps to limit the number of conditionals
    
    
    private int writeRuns(short[] data, int pos, byte[] source, int sourcePos, int sourceLength, int sourceMask) {
        if (sourceLength>0x7FFF || sourceLength<1) {
            throw new UnsupportedOperationException("does not support strings beyond this length "+0x7FFF);
        }
        int runLeft = sourceLength;
        data[pos++] = TYPE_RUN;
        data[pos++] = (short)runLeft;
        while (--runLeft >= 0) {
                data[pos++] = source[sourceMask & sourcePos++];
       }
       return pos;
    }

    
}
