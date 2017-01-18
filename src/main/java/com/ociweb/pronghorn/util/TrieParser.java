package com.ociweb.pronghorn.util;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;

/**
 * Optimized for fast lookup and secondarily size. 
 * Inserts may require data copy and this could be optimized in future releases if needed.
 * 
 * @author Nathan Tippy
 *
 */
public class TrieParser {
        
    
    private static final Logger logger = LoggerFactory.getLogger(TrieParser.class);

    static final byte TYPE_RUN                 = 0x00; //followed by length
    static final byte TYPE_BRANCH_VALUE        = 0x01; //followed by mask & short jump  
    static final byte TYPE_ALT_BRANCH          = 0X02; //followed by 2 short jump, try first upon falure use second.
    
    static final byte TYPE_VALUE_NUMERIC       = 0x04; //followed by type, parse right kind of number
    static final byte TYPE_VALUE_BYTES         = 0x05; //followed by stop byte, take all until stop byte encountered (AKA Wild Card)
            
    static final byte TYPE_SAFE_END            = 0X06;
    static final byte TYPE_END                 = 0x07;

    static final int BRANCH_JUMP_SIZE = 2;    
    
    static final int SIZE_OF_BRANCH               = 1+1+BRANCH_JUMP_SIZE; //type, branchon, jumpvalue
    static final int SIZE_OF_ALT_BRANCH           = 1  +BRANCH_JUMP_SIZE; //type,           jumpvalue
    
    static final int SIZE_OF_RUN                  = 1+1;

    final int SIZE_OF_RESULT;
    final int SIZE_OF_END_1;
    final int SIZE_OF_SAFE_END;
        
    
    static final int SIZE_OF_VALUE_NUMERIC        = 1+1; //second value is type mask
    static final int SIZE_OF_VALUE_BYTES          = 1+1; //second value is stop marker
    
    boolean skipDeepChecks;//these runs are not significant and do not provide any consumed data.
    //humans require long readable URLs but the machine can split them into categories on just a few key bytes
    
    public final byte ESCAPE_BYTE;
    public final byte NO_ESCAPE_SUPPORT=(byte)0xFF;
    
    
    //EXTRACT VALUE
    public static final byte ESCAPE_CMD_SIGNED_DEC    = 'i'; //signedInt (may be hex if starts with 0x)
    public static final byte ESCAPE_CMD_UNSIGNED_DEC  = 'u'; //unsignedInt (may be hex if starts with 0x)
    public static final byte ESCAPE_CMD_SIGNED_HEX    = 'I'; //signedInt (may skip prefix 0x)
    public static final byte ESCAPE_CMD_UNSIGNED_HEX  = 'U'; //unsignedInt (may skip prefix 0x) 
    public static final byte ESCAPE_CMD_DECIMAL       = '.'; //if found capture u and places else captures zero and 1 place
    public static final byte ESCAPE_CMD_RATIONAL      = '/'; //if found capture i else captures 1
    //EXTRACTED BYTES
    public static final byte ESCAPE_CMD_BYTES         = 'b';
      
    //////////////////////////////////////////////////////////////////////
    ///Every pattern is unaware of any context and can be mixed an any way.
    //////////////////////////////////////////////////////////////////////    
    // %%        a literal %
    // %i%.      unsigned value after dot in decimal and zero if not found   eg  3.75
    // %i%/      signed value after dot in hex and 1 if not found            eg  3/-4
    // %i%.%/%.  a rational number made up of two decimals                   eg  2.3/-1.7 
    // %bX       where X is the excluded stop short
    //////////////////////////////////////////////////////////////////////
    
    //numeric type bits:
    //   leading sign (only in front)
    static final byte NUMERIC_FLAG_SIGN     =  1;
    //   hex values can start with 0x, hex is all lower case abcdef
    static final byte NUMERIC_FLAG_HEX      =  2;
    //   starts with . if not return zero
    static final byte NUMERIC_FLAG_DECIMAL  =  4;
    //   starts with / if not return 1
    static final byte NUMERIC_FLAG_RATIONAL =  8;
    
    
    final short[] data;
    private int limit = 0;

    private final int MAX_TEXT_LENGTH = 4096;
    private Pipe<RawDataSchema> pipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance,3,MAX_TEXT_LENGTH));
    
    private int maxExtractedFields = 0;//out of all the byte patterns known what is the maximum # of extracted fields from any of them.
    
    
    private final static int MAX_ALT_DEPTH = 128;
    private int altStackPos = 0;
    private int[] altStackA = new int[MAX_ALT_DEPTH];
    private int[] altStackB = new int[MAX_ALT_DEPTH];
    
    
    public TrieParser(int size) {
        this(size, 1, true, true);
    }
    
    public TrieParser(int size, boolean skipDeepChecks) {
        this(size, 1, skipDeepChecks, true);
        
    }
    
    public TrieParser(int size, int resultSize, boolean skipDeepChecks, boolean supportsExtraction) {
    	this(size,resultSize,skipDeepChecks,supportsExtraction,false);
    }
    
    public TrieParser(int size, int resultSize, boolean skipDeepChecks, boolean supportsExtraction, boolean ignoreCase) {
        this.data = new short[size];
        this.pipe.initBuffers();
        
        this.SIZE_OF_RESULT               = resultSize;        //custom result size for this instance
        this.SIZE_OF_END_1                = 1+SIZE_OF_RESULT;
        this.SIZE_OF_SAFE_END             = 1+SIZE_OF_RESULT;//Same as end except we keep going and store this
        
        this.skipDeepChecks = skipDeepChecks;
                        
        if (supportsExtraction) {
            ESCAPE_BYTE = '%';
        } else {
            ESCAPE_BYTE = NO_ESCAPE_SUPPORT;
        }        
            	
        this.caseRuleMask =  ignoreCase ? (byte)0xDF : (byte)0xFF;   
    }
    
    
    public int getLimit() {
        return limit;
    }
    
    public void setSkipDeepChecks(boolean value) {
        skipDeepChecks = value;
    }
    
    public boolean isSkipDeepChecks() {
        return skipDeepChecks;
    }
    
    public void setValue(byte[] source, int offset, int length, int mask, long value) {
        setValue(0, data, source, offset, length, mask, value);        
    }
    
    public int getMaxExtractedFields() {
        return maxExtractedFields;
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
                case TYPE_ALT_BRANCH:
                    i = toStringAltBranch(builder, i);
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
                    int remaining = limit-i;
                    builder.append("ERROR Unrecognized value, remaining "+remaining+"\n");
                    if (remaining<100) {
                        builder.append("Remaining:"+Arrays.toString(Arrays.copyOfRange(data, i, limit))+"\n" );
                    }
                    
                    return builder;
            }            
        }
        return builder;
    }
    
    private int toStringSafe(StringBuilder builder, int i) {
        builder.append("SAFE");
        builder.append(data[i]).append("[").append(i++).append("], ");
        int s = SIZE_OF_RESULT;
        while (--s >= 0) {        
            builder.append(data[i]).append("[").append(i++).append("], ");
        }
        builder.append("\n");
        return i;
    }

    private int toStringNumeric(StringBuilder builder, int i) {
        builder.append("EXTRACT_NUMBER");
        builder.append(data[i]).append("[").append(i++).append("], ");
        
        builder.append(data[i]).append("[").append(i++).append("], \n");
        return i;
        
    }
    
    private int toStringBytes(StringBuilder builder, int i) {
        builder.append("EXTRACT_BYTES");
        builder.append(data[i]).append("[").append(i++).append("], ");
        
        builder.append(data[i]).append("[").append(i++).append("], \n");
        return i;
    }
    
    
    private int toStringEnd(StringBuilder builder, int i) {
        builder.append("END");
        builder.append(data[i]).append("[").append(i++).append("], ");
        int s = SIZE_OF_RESULT;
        while (--s >= 0) {        
            builder.append(data[i]).append("[").append(i++).append("], ");
        }
        builder.append("\n");
        return i;
    }


    private int toStringRun(StringBuilder builder, int i) {
        builder.append("RUN");
        builder.append(data[i]).append("[").append(i++).append("], ");
        int len = data[i];
        builder.append(data[i]).append("[").append(i++).append("], ");
        while (--len >= 0) {
            builder.append(data[i]);
            if ((data[i]>=32) && (data[i]<=126)) {
                builder.append("'").append((char)data[i]).append("'"); 
            }
            builder.append("[").append(i++).append("], ");
        }
        builder.append("\n");
        return i;
    }

    private int toStringAltBranch(StringBuilder builder, int i) {
        builder.append("ALT_BRANCH");
        builder.append(data[i]).append("[").append(i++).append("], "); //TYPE
                
        
      if (2==BRANCH_JUMP_SIZE) {
          //assert(data[i]>=0);
          builder.append(data[i]).append("[").append(i++).append("], ");
      }
      
        //assert(data[i]>=0);
        builder.append(data[i]).append("[").append(i++).append("], \n");//JUMP
        return i;
    }

    private int toStringBranchValue(StringBuilder builder, int i) {
        builder.append("BRANCH_VALUE");
        builder.append(data[i]).append("[").append(i++).append("], "); //TYPE
        
        builder.append(data[i]).append("[").append(i++).append("], "); //MASK FOR CHAR
        
        
      if (2==BRANCH_JUMP_SIZE) {
         // assert(data[i]>=0);
          builder.append(data[i]).append("[").append(i++).append("], ");
      }
      
      //  assert(data[i]>=0);
        builder.append(data[i]).append("[").append(i++).append("], \n");//JUMP
        return i;
    }

    
    public StringBuilder toDOT(StringBuilder builder) {
    	
    	builder.append("digraph {\n");    	
    	
        int i = 0;
        while (i<limit) {
        	
        	Appendables.appendValue(builder, "node", i, "[label=\"");
        	//each type will add its label details and close the line
        	//after closing the line links can also be added to other jump points       	
        	        	
            switch (data[i]) {
                case TYPE_SAFE_END:
                    i = toDotSafe(builder, i);
                    break;
                case TYPE_ALT_BRANCH:
                    i = toDotAltBranch(builder, i);
                    break;                    
                case TYPE_BRANCH_VALUE:
                    i = toDotBranchValue(builder, i);
                    break;
                case TYPE_VALUE_NUMERIC:
                    i = toDotNumeric(builder, i);
                    break;
                case TYPE_VALUE_BYTES:
                    i = toDotBytes(builder, i);
                    break;
                case TYPE_RUN:
                    i = toDotRun(builder, i);  
                    break;
                case TYPE_END:
                    i = toDotEnd(builder, i);
                    break;
                default:
                    int remaining = limit-i;
                    builder.append("ERROR Unrecognized value, remaining "+remaining+"\n");
                    if (remaining<100) {
                        builder.append("Remaining:"+Arrays.toString(Arrays.copyOfRange(data, i, limit))+"\n" );
                    }
                    
                    return builder;
            }            
        }
        
        builder.append("}\n");        
        
        return builder;
    }
    
    
    private int toDotSafe(StringBuilder builder, int i) {
        
    	int start = i;
    	
    	builder.append("SAFE");
        i++;//builder.append(data[i]).append("[").append(i++).append("], ");
        int s = SIZE_OF_RESULT;
        while (--s >= 0) {        
            builder.append(data[i]).append("[").append(i++).append("], ");
        }
        
        //end of label
        builder.append("\"]\n");
             
        Appendables.appendValue(builder,"node", start);
        builder.append("->");
        Appendables.appendValue(builder,"node", i, "\n"); //local
        
        
        return i;
    }

    private int toDotNumeric(StringBuilder builder, int i) {
        
    	int start = i;
    	
    	builder.append("EXTRACT_NUMBER");
        builder.append(data[i]).append("[").append(i++).append("], ");
        
        builder.append(data[i]).append("[").append(i++).append("]");
               
        
        //end of label
        builder.append("\"]\n");
        
        Appendables.appendValue(builder,"node", start);
        builder.append("->");
        Appendables.appendValue(builder,"node", i, "\n"); //local
        
        return i;
        
    }
    
    private int toDotBytes(StringBuilder builder, int i) {
    	
    	int start = i;
    	
        builder.append("EXTRACT_BYTES");
        builder.append(data[i]).append("[").append(i++).append("], ");
        
        builder.append(data[i]).append("[").append(i++).append("]");
        
        
        //end of label
        builder.append("\"]\n");
        
        Appendables.appendValue(builder,"node", start);
        builder.append("->");
        Appendables.appendValue(builder,"node", i, "\n"); //local
        
        return i;
    }
    
    
    private int toDotEnd(StringBuilder builder, int i) {
        builder.append("END");
        i++;//builder.append(data[i]).append("[").append(i++).append("], ");
        int s = SIZE_OF_RESULT;
        while (--s >= 0) {        
            builder.append(data[i]).append("[").append(i++).append("]");
        }        
        
        //end of label
        builder.append("\"]\n");        
        
        return i;
    }


    private int toDotRun(StringBuilder builder, int i) {
    	
    	int start = i;
    	
        //builder.append("RUN of ");
        i++;//builder.append(data[i]).append("[").append(i++).append("], ");
        int len = data[i];
        Appendables.appendValue(builder,"RUN of ", len, "\n");
        i++;//builder.append(data[i]).append("[").append(i++).append("]\n ");
                
        while (--len >= 0) {
        	        	            
        	if ((data[i]>=32) && (data[i]<=126)) {
                builder.append((char)data[i]); 
            } else {
            	builder.append("{").append(data[i]).append("}");
            }            
            i++;
        }        
        
        
        //end of label
        builder.append("\"]\n");
        
        Appendables.appendValue(builder,"node", start);
        builder.append("->");
        Appendables.appendValue(builder,"node", i, "\n"); //local
        
        return i;
    }

    private int toDotAltBranch(StringBuilder builder, int i) {
    	
    	int start = i;
    	
        builder.append("ALT_BRANCH");
        builder.append(data[i]).append("[").append(i++).append("], "); //TYPE
      
        //assert(data[i]>=0);
        builder.append(data[i]).append("[").append(i++).append("], ");//JUMP
        builder.append(data[i]).append("[").append(i++).append("]");  //JUMP
      
        
        //end of label
        builder.append("\"]\n");
                        
        //add jumps
        
        Appendables.appendValue(builder,"node", start);
        builder.append("->");
        Appendables.appendValue(builder,"node", i, "\n"); //local
        
        
        Appendables.appendValue(builder,"node", start);
        int destination = i + ((((int)data[i-2])<<15) | (0x7FFF&data[i-1]));
        builder.append("->");
        Appendables.appendValue(builder,"node", destination, "\n"); //jump
        
        
        return i;
    }

    private int toDotBranchValue(StringBuilder builder, int i) {
    	
    	int start = i;
    	
        builder.append("BRANCH ON BIT\n");
        i++;//  builder.append(data[i]).append("[").append(i++).append("], "); //TYPE
        
        builder.append(" bit:");
        String bits = ("00000000"+Integer.toBinaryString(data[i]));  //TODO: THIS IS A HACK FOR NOW, MOVE TO Appendables. we need binary support there.       
        builder.append(bits.substring(bits.length()-8,bits.length()));
        i++;//builder.append("[").append(i++).append("], "); //MASK FOR CHAR
      
        i++;//builder.append(data[i]).append("[").append(i++).append("], "); //JUMP
        i++;//builder.append(data[i]).append("[").append(i++).append("]");//JUMP
                
        //end of label
        builder.append("\"]\n");
        
        
        //add jumps
        
        Appendables.appendValue(builder,"node", start);
        builder.append("->");
        Appendables.appendValue(builder,"node", i, "\n"); //local
        
        Appendables.appendValue(builder,"node", start);
        int destination = i + ((((int)data[i-2])<<15) | (0x7FFF&data[i-1]));
        builder.append("->");
        Appendables.appendValue(builder,"node", destination, "\n"); //jump
                
        return i;
    }
   
    
    
    
    static int jumpOnBit(short source, short critera, int jump, int pos) {
    	
    	//System.out.println(" JumpOn Critera:  "+Integer.toBinaryString(critera));
    	
        return 1 + (( (~(((source & (0xFF & critera))-1)>>>8) ^ critera>>>8)) & jump) + pos;
    }

    public int setUTF8Value(CharSequence cs, int value) {
        
      //  pipe.reset();
        Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
        
        int origPos = Pipe.getBlobWorkingHeadPosition(pipe);
        int len = Pipe.copyUTF8ToByte(cs, 0, cs.length(), pipe);
        Pipe.addBytePosAndLen(pipe, origPos, len);        
        Pipe.publishWrites(pipe);
        Pipe.confirmLowLevelWrite(pipe, Pipe.sizeOf(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        Pipe.takeMsgIdx(pipe);
        setValue(pipe, value);
        Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        //WARNING: this is not thread safe if set is called and we have not yet parsed!!
        Pipe.releaseReadLock(pipe);
        return len;
        
    }
    
    public int setUTF8Value(CharSequence cs, CharSequence suffix, int value) {
        
        Pipe.addMsgIdx(pipe, 0);
        
        int origPos = Pipe.getBlobWorkingHeadPosition(pipe);
        int len = 0;
        len += Pipe.copyUTF8ToByte(cs, 0, cs.length(), pipe);        
        len += Pipe.copyUTF8ToByte(suffix, 0, suffix.length(), pipe);
                
        Pipe.addBytePosAndLen(pipe, origPos, len);
        Pipe.publishWrites(pipe);
        Pipe.confirmLowLevelWrite(pipe, Pipe.sizeOf(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        Pipe.takeMsgIdx(pipe);
        setValue(pipe, value);   
        Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        //WARNING: this is not thread safe if set is called and we have not yet parsed!!
        Pipe.releaseReadLock(pipe);
        return len;
    }


    public void setValue(Pipe p, long value) {
        setValue(p, Pipe.takeRingByteMetaData(p), Pipe.takeRingByteLen(p), value);
    }


    private void setValue(Pipe p, int meta, int length, long value) {
        setValue(0, data, Pipe.byteBackingArray(meta, p), Pipe.bytePosition(meta, p, length), length, Pipe.blobMask(p), value);
    }
       
    private int longestKnown = 0;
    private int shortestKnown = 0;

	public final byte caseRuleMask;
    
    public int longestKnown() {
    	return longestKnown;
    }
    
    public int shortestKnown() {
    	return shortestKnown;
    }
    
    private void setValue(int pos, short[] data, byte[] source, int sourcePos, final int sourceLength, int sourceMask, long value) {
        
    	longestKnown = Math.max(longestKnown, sourceLength);
    	shortestKnown = Math.max(shortestKnown, sourceLength);
    	
        assert(value >= 0);
        assert(value <= 0x7FFF_FFFF); 

        altStackPos = 0;
        int fieldExtractionsCount = 0;
        
        if (0!=limit) {
            int length = 0;
                    
            while (true) {
            
                int type = 0xFF & data[pos++];
                switch(type) {
                    case TYPE_BRANCH_VALUE:
                        
                        short v = (short) source[sourceMask & sourcePos];
                        if (NO_ESCAPE_SUPPORT!=ESCAPE_BYTE && ESCAPE_BYTE==v && ESCAPE_BYTE!=source[sourceMask & (1+sourcePos)] ) {
                            //we have found an escape sequence so we must insert a branch here we cant branch on a value
                            
                            fieldExtractionsCount++;
							final int sourcePos1 = sourcePos;
							final int sourceLength1 = sourceLength-length; 
                            assert(sourceLength1>=1);
							          
							writeEnd(data, writeRuns(data, insertAltBranch(0, data, pos-1, source, sourcePos1, sourceLength1, sourceMask), source, sourcePos1, sourceLength1, sourceMask), value); 
                            maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                            return;
                            
                        } else {
                            int pos1 = pos;
                            pos = jumpOnBit((short) v, data[pos1++], (((int)data[pos1++])<<15) | (0x7FFF&data[pos1]), pos1);   
                        
                        }
                        break;
                    case TYPE_ALT_BRANCH:
                 
                        altBranch(data, pos, sourcePos, (((int)data[pos++])<<15) | (0x7FFF&data[pos++]), data[pos]);
                                             
                        pos       = altStackA[--altStackPos];
                        sourcePos = altStackB[altStackPos];
                        
                        break;
                    case TYPE_VALUE_NUMERIC:   
                        fieldExtractionsCount++;
                        maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                        
                        if ('%'==source[sourceMask & sourcePos]) {                        	
                    		byte second = source[sourceMask & (sourcePos+1)];
                			if ('u'==second || 'U'==second ||
                				'.'==second || '/'==second ||
                				'i'==second || 'I'==second) {
                				
                				pos++;
                				length += 2;
                				sourcePos += 2;
                				
                				break;
                    		}
                    	}
					    final int insertLengthNumericCapture = sourceLength-length;
                        assert(insertLengthNumericCapture>=1);
					    writeEnd(data, writeRuns(data, insertAltBranch(0, data, pos-1, source, sourcePos, insertLengthNumericCapture, sourceMask), source, sourcePos, insertLengthNumericCapture, sourceMask), value);
                        return;

                    case TYPE_VALUE_BYTES:
                        fieldExtractionsCount++;       
                        maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                        
                    	if ('%'!=source[sourceMask & sourcePos]     ||
                    		'b'!=source[sourceMask & (sourcePos+1)] ||
                    		data[pos]!=source[sourceMask & (sourcePos+2)] ) {   	
                    		final int insertLengthBytesCapture = sourceLength-length;								
							assert(insertLengthBytesCapture>=1);					           
							writeEnd(data, writeRuns(data, insertAltBranch(0, data, pos-1, source, sourcePos, insertLengthBytesCapture, sourceMask), source, sourcePos, insertLengthBytesCapture, sourceMask), value);
                    		return;
                    		
                    	} else {
                    		pos++;//for the stop consumed
                    		length += 3;//move length forward by count of extracted bytes
                            sourcePos += 3;
                    	}
                                        
                        
                        break;
                    case TYPE_RUN:
                        //run
                        int runPos = pos++;
                        int run = data[runPos];
                              
                        int r = run;
                        if (sourceLength < run+length) {
                            
                            r = sourceLength-length;
                            assert(r<run);
                            int afterWhileLength = length+r;
                            int afterWhileRun    = run-r;
                            while (--r >= 0) {
                                byte sourceByte = source[sourceMask & sourcePos++];
                                
                                //found an escape byte, so this set may need to break the run up.
                                if (ESCAPE_BYTE == sourceByte && NO_ESCAPE_SUPPORT!=ESCAPE_BYTE) {
                                    sourceByte = source[sourceMask & sourcePos++];
             
                                    //confirm second value is not also the escape byte so we do have a command
                                    if (ESCAPE_BYTE != sourceByte) {
                                        fieldExtractionsCount++;
                                        
										insertAtBranchValueAlt(pos, data, source, sourceLength, sourceMask, value, length, runPos, run, r+afterWhileRun,	sourcePos-2); //TODO: this count can be off by buried extractions.      
									                                   
                                        maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                                        return;
                                    } else {
                                       sourcePos--;//found literal
                                    }
                                    //else we have two escapes in a row therefore this is a literal
                                }                                
                                
                                if (data[pos++] != sourceByte) {
                                    insertAtBranchValueByte(pos, data, source, sourceLength, sourceMask, value, length, runPos, run, r+afterWhileRun, sourcePos-1);    		
					
                                    maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                                    return;
                                }
                            }
                            length = afterWhileLength;
                            //matched up to this point but this was shorter than the run so insert a safe point
                            insertNewSafePoint(pos, data, source, sourcePos, afterWhileRun, sourceMask, value, runPos);     
                            maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                            return;
                        }                        
                        
                      //  int r = run;
                        while (--r >= 0) {
                            
                            byte sourceByte = source[sourceMask & sourcePos++];
                            if (ESCAPE_BYTE == sourceByte && NO_ESCAPE_SUPPORT==ESCAPE_BYTE) {
                                sourceByte = source[sourceMask & sourcePos++];
                   
                                if (ESCAPE_BYTE != sourceByte) {
                                    //sourceByte holds the specific command
                                    fieldExtractionsCount++;
									insertAtBranchValueAlt(pos+1, data, source, sourceLength, sourceMask, value, length, runPos, run, r,	sourcePos-2);
								                                       
                                    maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                                    return;
                                } else {
                                    sourcePos--; //found literal
                                }
                                //else we have two escapes in a row therefore this is a literal
                            }                            
                            
                            if (data[pos++] != sourceByte) {
                                insertAtBranchValueByte(pos, data, source, sourceLength, sourceMask, value, length, runPos, run, r, sourcePos-1);    		
			
                                maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                                return;
                            }
                        }
                        
                        length+=run;

                        break;
                    case TYPE_END:
                        
                        if (sourceLength>length) {
                            convertEndToNewSafePoint(pos, data, source, sourcePos, sourceLength-length, sourceMask, value);               
                        } else {
                            writeEndValue(data, pos, value);
                        }
                        maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount); //TODO: should this only be for the normal end??
                        return;
                        
                        
                    case TYPE_SAFE_END:
                        if (sourceLength>length) {
                            ///jump over the safe end values and continue on
                            pos += SIZE_OF_RESULT;
                            break;                            
                        } else {
                            pos = writeEndValue(data, pos, value);
                            maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                            return;
                        }
                    default:
                        System.out.println(this);
                        throw new UnsupportedOperationException("unknown op "+type+" at "+(pos-1));
                }
               
            }
        } else {
            //Start case where we insert the first run;
            pos = writeRuns(data, pos, source, sourcePos, sourceLength, sourceMask);
            limit = Math.max(limit, writeEnd(data, pos, value));
        }
        
        
    }

    void recurseAltBranch(short[] localData, int pos, int offset) {
        int type = localData[pos];
        if (type == TrieParser.TYPE_ALT_BRANCH) {
            
            pos++;
            if (1 == TrieParser.BRANCH_JUMP_SIZE ) {
                altBranch(localData, pos, offset, localData[pos++], localData[pos]);                                   
            } else {
                assert(localData[pos]>=0): "bad value "+localData[pos];
                assert(localData[pos+1]>=0): "bad value "+localData[pos+1];
                
                altBranch(localData, pos, offset, (((int)localData[pos++])<<15) | (0x7FFF&localData[pos++]), localData[pos]); 
            }
            
        } else {
            
            pushAlt(pos, offset);
            if (type == TrieParser.TYPE_VALUE_BYTES) {
                
                int j = 0;//TODO: can replace with keeping track of this value instead of scanning for it.
                while (j< altStackPos ) {
                    if (localData[altStackA[j]] != TrieParser.TYPE_VALUE_BYTES){
                        break;
                    }
                    j++;
                }
                
                if (j<altStackPos) {
                    
                    System.out.println("ZZZZ  now tested:"+j);//+" "+reader.altStackExtractCount);
                                        
                   // assert(j==altStackExtractCount);
                    
                    //swap j with reader.altStackPos-1;
                    int k = altStackPos-1;
                 
                    int a = altStackA[k];
                    int b = altStackB[k];
                    
                    altStackA[j] = a;
                    altStackB[j] = b;
                            
                   // altStackExtractCount++;
                }
                //TODO: when the top of the stack is a bytes extract keep peeking and take all the stop values together.
                
            }
            
            
            
            
            
        }
    }
    
    void altBranch(short[] localData, int pos, int offset, int jump, int peekNextType) {
        assert(jump>0) : "Jump must be postitive but found "+jump;
        
        //put extract first so its at the bottom of the stack
        if (TrieParser.TYPE_VALUE_BYTES == peekNextType || TrieParser.TYPE_VALUE_NUMERIC==peekNextType) {
            //Take the Jump value first, the local value has an extraction.
            //push the LocalValue
            recurseAltBranch(localData, pos+ TrieParser.BRANCH_JUMP_SIZE, offset);
            recurseAltBranch(localData, pos+jump+ TrieParser.BRANCH_JUMP_SIZE, offset);           
        } else {
            //Take the Local value first
            //push the JumpValue
            recurseAltBranch(localData, pos+jump+ TrieParser.BRANCH_JUMP_SIZE, offset);
            recurseAltBranch(localData, pos+ TrieParser.BRANCH_JUMP_SIZE, offset);
        }
    }
    
    
    private void pushAlt(int pos, int sourcePos) {
        altStackA[altStackPos] = pos;
        altStackB[altStackPos++] = sourcePos;

    }

    private byte buildNumberBits(byte sourceByte) { 
        
        switch(sourceByte) {
            case ESCAPE_CMD_SIGNED_DEC:
                return TrieParser.NUMERIC_FLAG_SIGN;
            case ESCAPE_CMD_UNSIGNED_DEC:
                return 0;
            case ESCAPE_CMD_SIGNED_HEX:
                return TrieParser.NUMERIC_FLAG_HEX | TrieParser.NUMERIC_FLAG_SIGN;
            case ESCAPE_CMD_UNSIGNED_HEX:
                return TrieParser.NUMERIC_FLAG_HEX;
            case ESCAPE_CMD_DECIMAL:
                return TrieParser.NUMERIC_FLAG_DECIMAL;
            case ESCAPE_CMD_RATIONAL:
                return TrieParser.NUMERIC_FLAG_SIGN | TrieParser.NUMERIC_FLAG_RATIONAL;
            default:
                throw new UnsupportedOperationException("Unsupported % operator found '"+((char)sourceByte)+"'");
        }
    }


    private void convertEndToNewSafePoint(int pos, short[] data, byte[] source, int sourcePos, int sourceLength, int sourceMask, long value) {
        //convert end to safe, pos is now at the location of SIZE_OF_RESULT data
        
        if (data[pos-1] != TYPE_END) {
            throw new UnsupportedOperationException();
        }
        data[--pos] = TYPE_SAFE_END; //change to a safe and move pos back to beginning of this.

        //now insert the needed run 
        int requiredRoom = SIZE_OF_END_1 + sourceLength + midRunEscapeValuesSizeAdjustment(source, sourcePos, sourceLength, sourceMask);             
      
		makeRoomForInsert(0, data, pos, requiredRoom); //after the safe point we make room for our new run and end
		pos += SIZE_OF_SAFE_END;

        pos = writeRuns(data, pos, source, sourcePos, sourceLength, sourceMask);        
        pos = writeEnd(data, pos, value);

    }

    /**
     * Compute the additional space needed for any value extraction meta command found in the middle of a run.
     */
    private int midRunEscapeValuesSizeAdjustment(byte[] source, int sourcePos, int sourceLength, int sourceMask) {
        
        if (0==sourceLength) {
            return 0;
        }
        
        int adjustment = 0;
        boolean needsRunStart = true;
        
        //int limit = sourceLength-sourcePos; //ERROR: 
        
        for(int i=0;i<sourceLength;i++) {
            
            byte value = source[sourceMask & (sourcePos+i)];
            
            if (ESCAPE_BYTE == value && NO_ESCAPE_SUPPORT!=ESCAPE_BYTE) {
                assert(value=='%');
                i++;
                value = source[sourceMask & (sourcePos+i)];
                if (ESCAPE_BYTE != value) {
                    if (ESCAPE_CMD_BYTES == value) { //%bX
                        i++;//skip over the X
                        adjustment--; //bytes is 2 but to request it is 3 so go down by one
                        
                    } else {
                        //no change
                        //all numerics are 2 but to request it is also 2 so no change.
                        
                    }
                    
                    needsRunStart = true;
                    
                    
                    //NOTE: in many cases this ends up creating 1 extra!!!!
                    
                } else {
                    //TODO: do store double escape?
                   //adjustment--; // we do not store double escape in the trie data
                }
            } else {
                if (needsRunStart) {
                    needsRunStart = false;
                    //for each escape we must add a new run header.
                    adjustment += SIZE_OF_RUN;
                }
                
            }
        }
        return adjustment;
    }


    private int insertNewSafePoint(int pos, short[] data, byte[] source, int sourcePos, int sourceLength, int sourceMask, long value, int runLenPos) {
        //convert end to safe
        
        makeRoomForInsert(sourceLength, data, pos, SIZE_OF_SAFE_END);
        
        data[pos++] = TYPE_SAFE_END;
        pos = writeEndValue(data, pos, value);

        pos = writeRunHeader(data, pos, sourceLength);
        data[runLenPos] -= sourceLength;//previous run is shortened buy the length of this new run
        return pos;
    }

	private void insertAtBranchValueAlt(final int pos, short[] data, byte[] source, int sourceLength, int sourceMask,
			long value, int length, int runPos, int run, int r1, final int sourceCharPos) {
		r1++;
		if (r1 == run) {
			final int insertLength = sourceLength - length;
			assert(insertLength>=1);
		           
			writeEnd(data, writeRuns(data, insertAltBranch(0, data, pos>=3 ? pos-3 : 0, source, sourceCharPos, insertLength, sourceMask), source, sourceCharPos, insertLength, sourceMask), value);
		} else {
			final int insertLength = sourceLength - (length+(data[runPos] = (short)(run-r1)));
			assert(insertLength>=1);
           
			writeEnd(data, writeRuns(data, insertAltBranch(r1, data, pos-1, source, sourceCharPos, insertLength, sourceMask), source, sourceCharPos, insertLength, sourceMask), value);
		}
	}

	private void insertAtBranchValueByte(final int pos, short[] data, byte[] source, int sourceLength, int sourceMask,
			long value, int length, int runPos, int run, int r1, final int sourceCharPos) {
		r1++;
		if (r1 == run) {
			final int sourceLength1 = sourceLength - length;
			assert(sourceLength1>=1);
			writeEnd(data, writeRuns(data, insertByteBranch(0, data, pos>=3 ? pos-3 : 0, source, sourceCharPos, sourceLength1, sourceMask), source, sourceCharPos, sourceLength1, sourceMask), value);
		} else {
			final int sourceLength1 = sourceLength - (length+(data[runPos] = (short)(run-r1)));
			assert(sourceLength1>=1);
			writeEnd(data, writeRuns(data, insertByteBranch(r1, data, pos-1, source, sourceCharPos, sourceLength1, sourceMask), source, sourceCharPos, sourceLength1, sourceMask), value);
		}
	}


    private int stepOverBytes(byte[] source, int sourcePos, int sourceMask, final short stop) {
    	
    	//TODO: WHOW TO DO WE SUPPORT THE END WITH NO STOP??
    	
    	if (ESCAPE_BYTE==source[sourceMask & sourcePos] && NO_ESCAPE_SUPPORT!=ESCAPE_BYTE) {        	
    		byte second = source[sourceMask & (sourcePos+1)];
    		if ('b'==second) {				
				byte third = source[sourceMask & (sourcePos+2)];				
				if (stop == third) {
					return sourcePos+3;
				}
    		}
    	}
    	
        short t = 0;
        int c = source.length;
        do {
            t = source[sourceMask & sourcePos++];
        }  while (stop!=t && --c>0);
        return stop==t ? sourcePos : -1;
    }


    private int stepOverNumeric(byte[] source, int sourcePos, int sourceMask, int numType) {

    	if (ESCAPE_BYTE==source[sourceMask & sourcePos] && NO_ESCAPE_SUPPORT!=ESCAPE_BYTE) {
    	
    		byte second = source[sourceMask & (sourcePos+1)];
			if ('u'==second || 'U'==second ||
				'.'==second || '/'==second ||
				'i'==second || 'I'==second
				) {
    	    	return sourcePos+2;
    		}
    	}
    	
    	
        //NOTE: these Numeric Flags are invariants consuming runtime resources, this tree could be pre-compiled to remove them if neded.
        if (0!=(NUMERIC_FLAG_SIGN&numType)) {
            final short c = source[sourceMask & sourcePos];
            if (c=='-' || c=='+') {
                sourcePos++;
            }                         
        }
                         
        boolean hasNo0xPrefix = ('0'!=source[sourceMask & sourcePos+1]) || ('x'!=source[sourceMask & sourcePos+2]);
		if (hasNo0xPrefix && 0==(NUMERIC_FLAG_HEX&numType) ) {                            
            short c = 0;
            do {
                c = source[sourceMask & sourcePos++];
            }  while ((c>='0') && (c<='9'));
        } else {
        	if (!hasNo0xPrefix) {
        		sourcePos+=2;//skipping over the 0x checked above
        	}
            short c = 0;
            do {
                c = source[sourceMask & sourcePos++];
            }  while (((c>='0') && (c<='9')) | ((c>='a') && (c<='f'))  );
        }

        return sourcePos;
    }


//    private void insertAtBranchValue(int danglingByteCount, short[] data, int pos, byte[] source, final int sourcePos,final int sourceLength, int sourceMask, long value, boolean branchOnByte) {
//
//        assert(sourceLength>=1);
//   
//        if (branchOnByte) {        
//            pos = insertByteBranch(danglingByteCount, data, pos, source, sourcePos, sourceLength, sourceMask);
//        } else {            
//            pos = insertAltBranch(danglingByteCount, data, pos, source, sourcePos, sourceLength, sourceMask);            
//        }       
//
//        writeEnd(data, writeRuns(data, pos, source, sourcePos, sourceLength, sourceMask), value);
//        
//    }

	private int insertByteBranch(int danglingByteCount, short[] data, int pos, byte[] source, final int sourcePos,
			final int sourceLength, int sourceMask) {
		final int requiredRoom = SIZE_OF_END_1 + SIZE_OF_BRANCH + sourceLength + midRunEscapeValuesSizeAdjustment(source, sourcePos, sourceLength, sourceMask);
		            		
		final int oldValueIdx = makeRoomForInsert(danglingByteCount, data, pos, requiredRoom);
		pos = writeBranch(TYPE_BRANCH_VALUE, data, pos, requiredRoom, findSingleBitMask((short) source[sourcePos & sourceMask], data[oldValueIdx]));
		return pos;
	}

	private int insertAltBranch(int danglingByteCount, short[] data, int pos, byte[] source, final int sourcePos, final int sourceLength, int sourceMask) {
		
		int requiredRoom = SIZE_OF_END_1 + SIZE_OF_ALT_BRANCH + sourceLength + midRunEscapeValuesSizeAdjustment(source, sourcePos, sourceLength, sourceMask);  
		final int oldValueIdx = makeRoomForInsert(danglingByteCount, data, pos, requiredRoom);

		requiredRoom -= SIZE_OF_ALT_BRANCH;//subtract the size of the branch operator
		data[pos++] = TYPE_ALT_BRANCH;         
		
		assert(2==BRANCH_JUMP_SIZE);              
		data[pos++] = (short)(0x7FFF&(requiredRoom>>15));          
		data[pos++] = (short)(0x7FFF&requiredRoom);
		return pos;
	}


    private short findSingleBitMask(short a, short b) {
        int mask = 1<<5; //default of sign bit, only used when nothing replaces it. (critical for case insensitivity)       
        int i = 8; 
        while (--i>=0) {            
            if (5!=i) { //sign bit, we do not use it unless all the others are tested first                
                int localMask = 1 << i;
                if ((localMask&a) != (localMask&b)) {
                	mask = localMask;
                    break;
                }
            }          
        }        
        return (short)(( 0xFF00&((mask&b)-1) ) | mask); //high byte is on when A matches mask
    }

    private int makeRoomForInsert(int danglingByteCount, short[] data, int pos, int requiredRoom) {
                
    	
        int len = limit - pos;
        if (danglingByteCount > 0) {
            requiredRoom+=SIZE_OF_RUN; //added because we will prepend this with a TYPE_RUN header to close the dangling bytes
        }
        limit+=requiredRoom;      
        
        if (len <= 0) {
            return pos;//nothing to be moved
        }                

        updatePreviousJumpDistances(0, data, pos, requiredRoom);        
        
        int newPos = pos + requiredRoom;
        assert(pos>=0);
        assert(pos+len<data.length);
        if (newPos+len>data.length) {
        	throw new UnsupportedOperationException("allocated length of "+data.length+" is too short to add all the patterns");
        }       
        System.arraycopy(data, pos, data, newPos, len);
        
        if (danglingByteCount > 0) {//do the prepend because we now have room
            data[newPos-2] = TYPE_RUN;
            data[newPos-1] = (short)danglingByteCount;
        } else {
            //new position already has the start of run so move cursor up to the first data point 
            newPos+=SIZE_OF_RUN;
        }
        return newPos;
    }


    private void updatePreviousJumpDistances(int i, short[] data, int limit, int requiredRoom) {

        while (i<limit) {
       
            switch (data[i]) {
                case TYPE_SAFE_END:
                    i += SIZE_OF_SAFE_END;
                    break;
                case TYPE_BRANCH_VALUE:
                    {
                        int jmp = (((int)data[i+2]) << 15)|(0x7FFF&data[i+3]);
                        
                        int newPos = SIZE_OF_BRANCH+i+jmp;
                        if (newPos > limit) {
                            
                        	//System.err.println("byte jmp "+ jmp+" adjusted to new jump of "+(jmp+requiredRoom));
                        	//System.err.println("byte jmp target "+ (i+4+jmp)+" adjusted to new jump targe of "+(i+4+jmp+requiredRoom));
                        	
                        	
                            //adjust this value because it jumps over the new inserted block
                            jmp += requiredRoom; 
                            
                            data[i+2] = (short)(0x7FFF&(jmp>>15));
                            data[i+3] = (short)(0x7FFF&(jmp));

                            
                            
                            
                        }
                        i += SIZE_OF_BRANCH;
                    }
                    break;     
                case TYPE_ALT_BRANCH:
                    {
                        int jmp = (((int)data[i+1]) << 15)|(0x7FFF&data[i+2]);
                                       
                           int newPos = SIZE_OF_ALT_BRANCH+i+jmp;
                           if (newPos > limit) {
                        	   
                        	   //System.err.println("alt jmp "+jmp+" adjusted to new jump of "+(jmp+requiredRoom));
                        	   
                               //adjust this value because it jumps over the new inserted block
                               jmp += requiredRoom; 
             
                               data[i+1] = (short)(0x7FFF&(jmp>>15));
                               data[i+2] = (short)(0x7FFF&(jmp));
                                                          
                           }
                           i += SIZE_OF_ALT_BRANCH;
                    }
                    break;
                case TYPE_VALUE_NUMERIC:
                    i += SIZE_OF_VALUE_NUMERIC;
                    break;
                case TYPE_VALUE_BYTES:
                    i += SIZE_OF_VALUE_BYTES;
                    break;                    
                case TYPE_RUN:
                	assert(data[i+1] >= 0) : "run length must be positive but we found "+data[i+1]+" at position "+i;              
                    i = i+SIZE_OF_RUN+data[i+1];
                    break;
                case TYPE_END:
                    i += SIZE_OF_END_1;
                    break;
                default:
                    System.out.println(this);
                    throw new UnsupportedOperationException("ERROR Unrecognized value "+data[i]+" at "+i);
            }            
        }
    }


    private int writeBranch(byte type, short[] data, int pos, int requiredRoom, short criteria) {
                
        requiredRoom -= SIZE_OF_BRANCH;//subtract the size of the branch operator
        data[pos++] = type;
        data[pos++] = criteria;
        
        assert(2==BRANCH_JUMP_SIZE);
        data[pos++] = (short)(0x7FFF&(requiredRoom>>15));
        data[pos++] = (short)(0x7FFF&requiredRoom);

        return pos;
    }


    private int writeEnd(short[] data, int pos, long value) {
        data[pos++] = TYPE_END;
        return writeEndValue(data, pos, value);
    }


    private int writeEndValue(short[] data, int pos, long value) {
        
        int s = SIZE_OF_RESULT;
        while (--s >= 0) {        
            data[pos++] = (short)(0xFFFF& (value>>(s<<4)));        
        }
        return pos;
    }
    
    static long readEndValue(short[] data, int pos, int resultSize) {
        
        long result = 0;
        int s = resultSize;
        while (--s >= 0) {            
            result = (result<<16) | (0xFFFFL & data[pos++]);        
        }
        
        return result;
    }
 
    private int writeBytesExtract(short[] data, int pos, short stop) {
        data[pos++] = TYPE_VALUE_BYTES;
        data[pos++] = stop;
        return pos;
    }
    
    private int writeNumericExtract(short[] data, int pos, int type) {
        data[pos++] = TYPE_VALUE_NUMERIC;
        data[pos++] = buildNumberBits((byte)type);
        return pos;
    }
 
    private int writeRuns(short[] data, int pos, byte[] source, int sourcePos, int sourceLength, int sourceMask) {
       if (0 == sourceLength) {
           return pos;
       }
       
       assert(ESCAPE_BYTE != source[sourceMask & (sourcePos+sourceLength-1)]) : "Escape byte is always followed by something and can not be last.";
              
       pos = writeRunHeader(data, pos, sourceLength);
       int runLenPos = pos-1;
       int runLeft = sourceLength;
       int sourceStop = sourceLength+sourcePos;
       short activeRunLength = 0;
       while (--runLeft >= 0) {
                  byte value = source[sourceMask & sourcePos++];
                  if (ESCAPE_BYTE == value && NO_ESCAPE_SUPPORT!=ESCAPE_BYTE) {
                      assert(value=='%');
                      value = source[sourceMask & sourcePos++];
                      if (ESCAPE_BYTE != value) {
                          //new command so we must stop the run at this point
                          if (activeRunLength > 0) {
                              data[runLenPos]=activeRunLength; //this run has ended so we must set the new length.      
                          } else {
                              //wipe out run because we must start with extraction
                              pos = runLenPos-1;
                          }
                          
                          if (ESCAPE_CMD_BYTES == value) {
                              byte stop = source[sourceMask & sourcePos++];
                              pos = writeBytesExtract(data, pos, stop);
                              
                              //Recursion used to complete the rest of the run.
                              int remainingLength = runLeft-2;
                              if (remainingLength > 0) {
                                  pos = writeRuns(data, pos, source, sourcePos, remainingLength, sourceMask);
                              }
                          } else {
                              pos = writeNumericExtract(data, pos, value);                                                            
                              int remainingLength = runLeft-1;                                                         
                              if (remainingLength > 0) {
                                  pos = writeRuns(data, pos, source, sourcePos, remainingLength, sourceMask);
                              }
                          }
                          return pos;
                      } else {
                          //add this value twice 
                          data[pos++] = value;
                          activeRunLength++;
                          //literal so jump over the second instance
                          sourcePos++;
                         
                      }
                  }
                  data[pos++] = value;
                  activeRunLength++;
                
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

	public void setValue(byte[] bytes, long value) {
		setValue(bytes, 0, bytes.length, Integer.MAX_VALUE, value);
		
	}



    
}
