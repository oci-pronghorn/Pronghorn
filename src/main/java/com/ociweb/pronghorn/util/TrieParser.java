package com.ociweb.pronghorn.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;

/**
 * Optimized for fast lookup and secondarily size. 
 * Inserts may require data copy and this could be optimized in future releases if needed.
 * 
 * @author Nathan Tippy
 *
 */
public class TrieParser implements Serializable {
        
    

	private static final long serialVersionUID = -2877089562575447986L;

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
    public static final byte ESCAPE_CMD_OPTIONAL_SIGNED_INT = 'o'; //optional signed int, if absent returns zero
    public static final byte ESCAPE_CMD_SIGNED_INT          = 'i'; //signedInt (may be hex if starts with 0x)
    public static final byte ESCAPE_CMD_UNSIGNED_INT        = 'u'; //unsignedInt (may be hex if starts with 0x)
    public static final byte ESCAPE_CMD_SIGNED_HEX          = 'I'; //signedInt (may skip prefix 0x, assumed to be hex)
    public static final byte ESCAPE_CMD_UNSIGNED_HEX        = 'U'; //unsignedInt (may skip prefix 0x, assumed to be hex) 
    public static final byte ESCAPE_CMD_DECIMAL             = '.'; //if found capture u and places else captures zero and 1 place
    public static final byte ESCAPE_CMD_RATIONAL            = '/'; //if found capture i else captures 1
    //EXTRACTED BYTES
    public static final byte ESCAPE_CMD_BYTES               = 'b';
      
    //////////////////////////////////////////////////////////////////////
    ///Every pattern is unaware of any context and can be mixed an any way.
    //////////////////////////////////////////////////////////////////////    
    // %%        a literal %
    // %i%.      unsigned value after dot in decimal and zero if not found   eg  3.75
    // %i%/      signed value after dot in hex and 1 if not found            eg  3/-4
    // %i%.%/%.  a rational number made up of two decimals                   eg  2.3/-1.7 
    // %bX       where X is the excluded stop short
    //////////////////////////////////////////////////////////////////////
    
    //TODO: add support for %Ni where we capture a number of fixed length N
    //      this will be for HHmmSS extraction into 3 fields...
    
    //numeric type bits:
    //   leading sign (only in front)
    static final short NUMERIC_FLAG_SIGN           =  1;
    //   hex values can start with 0x, hex is all lower case abcdef
    static final short NUMERIC_FLAG_HEX            =  2;
    //   starts with . if not return zero
    static final short NUMERIC_FLAG_DECIMAL        =  4;
    //   starts with / if not return 1
    static final short NUMERIC_FLAG_RATIONAL       =  8;
    //   when there is no number take that path and use zero as the value
    static final short NUMERIC_FLAG_ABSENT_IS_ZERO =  (short)0x8000;
    
    
    
    
    private final boolean fixedSize;
    short[] data; 
    private int limit = 0;

    private final int MAX_TEXT_LENGTH = 1024;
    private transient Pipe<RawDataSchema> workingPipe = RawDataSchema.instance.newPipe(2,MAX_TEXT_LENGTH);
    
    private int maxExtractedFields = 0;//out of all the byte patterns known what is the maximum # of extracted fields from any of them.

    private final static int MAX_ALT_DEPTH = 128;
    private int altStackPos = 0;
    private int[] altStackA = new int[MAX_ALT_DEPTH];
    private int[] altStackB = new int[MAX_ALT_DEPTH];
    
    int extractionCount;
    byte[] extractions = new byte[32];//hard coded limit of extraction points, could be larger but why...
    
    /**
     * Provides visibility into which fields will be extracted from the last known pattern
     */
    public byte[] lastSetValueExtractonPattern() {
    	return Arrays.copyOfRange(extractions, 0, extractionCount);
    }    
    
    public int lastSetValueExtractionCount() {
    	return extractionCount;
    }

	//used for detection of parse errors, eg do we need more data or did something bad happen.
	private int maxBytesCapturable      = 500; //largest text
	private int maxNumericLenCapturable = 20; //largest numeric.
    

	public TrieParser() {
		this(256);
	}
    
    public TrieParser(int size) {
        this(size, 1, false, true);
    }
    
    public TrieParser(int size, boolean skipDeepChecks) {
        this(size, 1, skipDeepChecks, true);
    }
    
    public TrieParser(int size, int resultSize, boolean skipDeepChecks, boolean supportsExtraction) {
    	this(size,resultSize,skipDeepChecks,supportsExtraction,false);
    }
    
    public TrieParser(int size, int resultSize, boolean skipDeepChecks, boolean supportsExtraction, boolean ignoreCase) {
    	this(size,resultSize,skipDeepChecks,supportsExtraction,ignoreCase,(byte)'%'); //default escape is set here.
    }
    public TrieParser(int size, int resultSize, boolean skipDeepChecks, boolean supportsExtraction, boolean ignoreCase, byte customEscape) {
        this.data = new short[size];
        this.fixedSize = false; //if its not fixed size then the .data array will grow as needed.
        
        this.workingPipe.initBuffers();
        
        this.SIZE_OF_RESULT               = resultSize;        //custom result size for this instance
        this.SIZE_OF_END_1                = 1+SIZE_OF_RESULT;
        this.SIZE_OF_SAFE_END             = 1+SIZE_OF_RESULT;//Same as end except we keep going and store this
        
        this.skipDeepChecks = skipDeepChecks;
                        
        if (supportsExtraction) {
        	assert(customEscape!=NO_ESCAPE_SUPPORT);
            ESCAPE_BYTE = customEscape; //set custom escape char for the case that we need to use %
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
        setValue(0, source, offset, length, mask, value);        
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
                
        //assert(data[i]>=0);
        builder.append(data[i]).append("[").append(i++).append("], ");
           
        //assert(data[i]>=0);
        builder.append(data[i]).append("[").append(i++).append("], \n");//JUMP
        return i;
    }

    private int toStringBranchValue(StringBuilder builder, int i) {
        builder.append("BRANCH_VALUE");
        builder.append(data[i]).append("[").append(i++).append("], "); //TYPE
        
        builder.append(data[i]).append("[").append(i++).append("], "); //MASK FOR CHAR
                
        // assert(data[i]>=0);
        builder.append(data[i]).append("[").append(i++).append("], ");
           
        //  assert(data[i]>=0);
        builder.append(data[i]).append("[").append(i++).append("], \n");//JUMP
        return i;
    }

    
    public <A extends Appendable> A toDOT(A builder) {
    
    	try{    	
    		//builder.append("# dot -Tsvg -otemp.svg temp.dot\n");
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
    	} catch (IOException ioex) {
    		throw new RuntimeException(ioex);
    	}
        
        return builder;
    }
    
    
    private int toDotSafe(Appendable builder, int i) throws IOException {
        
    	int start = i;
    	
    	builder.append("SAFE");
        i++;//builder.append(data[i]).append("[").append(i++).append("], ");
        int s = SIZE_OF_RESULT;
        while (--s >= 0) {        
            Appendables.appendValue(builder, data[i]);
            builder.append("[");
            Appendables.appendValue(builder,i++);
            builder.append("], ");
        }
        
        //end of label
        builder.append("\"]\n");
             
        Appendables.appendValue(builder,"node", start);
        builder.append("->");
        Appendables.appendValue(builder,"node", i, "\n"); //local
        
        
        return i;
    }

    private int toDotNumeric(Appendable builder, int i) throws IOException {
        
    	int start = i;
    	
    	builder.append("EXTRACT_NUMBER");
        
    	Appendables.appendValue(builder, data[i]);       
        builder.append("[");
        Appendables.appendValue(builder, i++);
        builder.append("], ");
        
        Appendables.appendValue(builder, data[i]);       
        builder.append("[");
        Appendables.appendValue(builder, i++);
        builder.append("]");
               
        
        //end of label
        builder.append("\"]\n");
        
        Appendables.appendValue(builder,"node", start);
        builder.append("->");
        Appendables.appendValue(builder,"node", i, "\n"); //local
        
        return i;
        
    }
    
    private int toDotBytes(Appendable builder, int i) throws IOException {
    	
    	int start = i;
    	
        builder.append("EXTRACT_BYTES");
        Appendables.appendValue(builder,data[i]).append("[");
        Appendables.appendValue(builder,i++).append("], ");
        
        Appendables.appendValue(builder,data[i]).append("[");
        Appendables.appendValue(builder,i++).append("]");
        
        
        //end of label
        builder.append("\"]\n");
        
        Appendables.appendValue(builder,"node", start);
        builder.append("->");
        Appendables.appendValue(builder,"node", i, "\n"); //local
        
        return i;
    }
    
    
    private int toDotEnd(Appendable builder, int i) throws IOException {
        builder.append("END");
        i++;//builder.append(data[i]).append("[").append(i++).append("], ");
        int s = SIZE_OF_RESULT;
        while (--s >= 0) {        
        	Appendables.appendValue(builder,data[i]).append("[");
        	Appendables.appendValue(builder,i++).append("]");
        }        
        
        //end of label
        builder.append("\"]\n");        
        
        return i;
    }


    private int toDotRun(Appendable builder, int i) throws IOException {
    	
    	int start = i;
    	
        //builder.append("RUN of ");
        i++;//builder.append(data[i]).append("[").append(i++).append("], ");
        int len = data[i];
        Appendables.appendValue(builder,"RUN of ", len, "\n");
        i++;//builder.append(data[i]).append("[").append(i++).append("]\n ");
                
        while (--len >= 0) {
        	        	            
        	if ((data[i]>=32) && (data[i]<=126)) {
                char c = (char)data[i];
                if (c=='"' || c=='\\') {
                	builder.append('\\');
                }                
				builder.append(c); 
            } else {
            	builder.append("{");
            	Appendables.appendValue(builder,data[i]).append("}");
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

    private int toDotAltBranch(Appendable builder, int i) throws IOException {
    	
    	int start = i;
    	
        builder.append("ALT_BRANCH");
        Appendables.appendValue(builder,data[i]);
        builder.append("[");
        Appendables.appendValue(builder,i++).append("], "); //TYPE
      
        //assert(data[i]>=0);
        Appendables.appendValue(builder,data[i]).append("[");
        Appendables.appendValue(builder,i++).append("], ");//JUMP
        Appendables.appendValue(builder,data[i]).append("[");
        Appendables.appendValue(builder,i++).append("]");  //JUMP
      
        
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

    private int toDotBranchValue(Appendable builder, int i) throws IOException {
    	
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
       
    
    
    static int computeJumpMask(short source, short critera) {
		return ~(((source & (0xFF & critera))-1)>>>8) ^ critera>>>8;
	}

    public int setUTF8Value(CharSequence cs, long value) {
        
    	if (cs.length()<<3 > workingPipe.maxVarLen) {
    		workingPipe = RawDataSchema.instance.newPipe(2,cs.length());
    		workingPipe.initBuffers();
    	}  
    	
        Pipe.addMsgIdx(workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
        
        int origPos = Pipe.getWorkingBlobHeadPosition(workingPipe);
        int len = Pipe.copyUTF8ToByte(cs, 0, cs.length(), workingPipe);
        Pipe.addBytePosAndLen(workingPipe, origPos, len);        
        Pipe.publishWrites(workingPipe);
        Pipe.confirmLowLevelWrite(workingPipe, Pipe.sizeOf(workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        Pipe.takeMsgIdx(workingPipe);
        setValue(workingPipe, value);
        Pipe.confirmLowLevelRead(workingPipe, Pipe.sizeOf(workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        //WARNING: this is not thread safe if set is called and we have not yet parsed!!
        Pipe.releaseReadLock(workingPipe);
        return len;
        
    }
    
    public int setUTF8Value(CharSequence cs, CharSequence suffix, long value) {
        
    	if ((cs.length()+suffix.length())<<3 > workingPipe.maxVarLen) {
    		workingPipe = RawDataSchema.instance.newPipe(2,suffix.length());
    		workingPipe.initBuffers();
    	}     	
    	
        Pipe.addMsgIdx(workingPipe, 0);
        
        int origPos = Pipe.getWorkingBlobHeadPosition(workingPipe);
        int len = 0;
        len += Pipe.copyUTF8ToByte(cs, 0, cs.length(), workingPipe);        
        len += Pipe.copyUTF8ToByte(suffix, 0, suffix.length(), workingPipe);
                
        Pipe.addBytePosAndLen(workingPipe, origPos, len);
        Pipe.publishWrites(workingPipe);
        Pipe.confirmLowLevelWrite(workingPipe, Pipe.sizeOf(workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        Pipe.takeMsgIdx(workingPipe);
        setValue(workingPipe, value);   
        Pipe.confirmLowLevelRead(workingPipe, Pipe.sizeOf(workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        //WARNING: this is not thread safe if set is called and we have not yet parsed!!
        Pipe.releaseReadLock(workingPipe);
        return len;
    }

    public int setUTF8Value(CharSequence prefix, CharSequence cs, CharSequence suffix, long value) {
        
    	if (((prefix.length() + cs.length() + suffix.length()) << 3) > workingPipe.maxVarLen) {
    		workingPipe = RawDataSchema.instance.newPipe(2,suffix.length());
    		workingPipe.initBuffers();
    	}   
    	
        Pipe.addMsgIdx(workingPipe, 0);
        
        int origPos = Pipe.getWorkingBlobHeadPosition(workingPipe);
        int len = 0;
        len += Pipe.copyUTF8ToByte(prefix, 0, prefix.length(), workingPipe);
        len += Pipe.copyUTF8ToByte(cs, 0, cs.length(), workingPipe);        
        len += Pipe.copyUTF8ToByte(suffix, 0, suffix.length(), workingPipe);
                
        Pipe.addBytePosAndLen(workingPipe, origPos, len);
        Pipe.publishWrites(workingPipe);
        Pipe.confirmLowLevelWrite(workingPipe, Pipe.sizeOf(workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        Pipe.takeMsgIdx(workingPipe);
        setValue(workingPipe, value);   
        Pipe.confirmLowLevelRead(workingPipe, Pipe.sizeOf(workingPipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        
        //WARNING: this is not thread safe if set is called and we have not yet parsed!!
        Pipe.releaseReadLock(workingPipe);
        return len;
    }
    

    public void setValue(Pipe p, long value) {
        setValue(p, Pipe.takeRingByteMetaData(p), Pipe.takeRingByteLen(p), value);
    }


    private void setValue(Pipe p, int meta, int length, long value) {
        setValue(0, Pipe.byteBackingArray(meta, p), Pipe.bytePosition(meta, p, length), length, Pipe.blobMask(p), value);
    }
       
    
	//since this is an alt one of the 3 alt values must exist
	//these are set up so that we prefer the type with the lowest (to the left) index over later ones
	private static final int[] captureBytesChoices = 
	      new int[]{TrieParser.TYPE_VALUE_BYTES,TrieParser.TYPE_VALUE_NUMERIC,TrieParser.TYPE_ALT_BRANCH};
	
	private static final int[] captureNumberChoices = 
  	      new int[]{TrieParser.TYPE_VALUE_NUMERIC,TrieParser.TYPE_VALUE_BYTES,TrieParser.TYPE_ALT_BRANCH};
  	
	private static final int[] definedChoices = 
    	      new int[]{TrieParser.TYPE_RUN, TrieParser.TYPE_END, TrieParser.TYPE_SAFE_END, TrieParser.TYPE_BRANCH_VALUE  
    	    		   ,TrieParser.TYPE_VALUE_BYTES,TrieParser.TYPE_VALUE_NUMERIC,TrieParser.TYPE_ALT_BRANCH};
    	
	
    private int longestKnown = 0;
    private int shortestKnown = Integer.MAX_VALUE;

	public final byte caseRuleMask;
    
    public int longestKnown() {
    	return longestKnown;
    }
    
    public int shortestKnown() {
    	return shortestKnown;
    }
    
    private void setValue(int pos, byte[] source, int sourcePos, final int sourceLength, int sourceMask, long value) {
 
    	extractionCount = 0;//clear this so it can be requested after set is complete.
    	longestKnown = Math.max(longestKnown, computeMax(source, sourcePos, sourceLength, sourceMask));
    	shortestKnown = Math.min(shortestKnown, sourceLength);
    	
        assert(value >= 0);

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
							          
							writeEnd(writeRuns(insertAltBranch(0, pos-1, source, sourcePos1, sourceLength1, sourceMask), source, sourcePos1, sourceLength1, sourceMask), value); 
                            maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                            return;
                            
                        } else {
                     
                            int pos1 = pos;
							int jumpMask = computeJumpMask((short) v, data[pos1++]);														
                            pos = 0==jumpMask? 1+pos1 : 1+(jumpMask&((((int)data[pos1++])<<15) | (0x7FFF&data[pos1])))+pos1;   
                        
                        }
                        break;
                    case TYPE_ALT_BRANCH:

                        altBranch(pos, sourcePos, (((int)data[pos++])<<15) | (0x7FFF&data[pos++]), data[pos]);

					    int[] choices = selectDataSpecificOrderedChoices(source, sourcePos, sourceMask);
                     
					    int selectedStackPos = choseOptimalPathFromStack(choices);
                     
					    altStackPos = 0;
                        
                        //pop top off stack
                        pos       = altStackA[selectedStackPos];
                        sourcePos = altStackB[selectedStackPos];
                
                        break;
                    case TYPE_VALUE_NUMERIC:   
                        fieldExtractionsCount++;
                        maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                        
                        if (ESCAPE_BYTE==source[sourceMask & sourcePos]) {                        	
                    		byte second = source[sourceMask & (sourcePos+1)];
                    		extractions[extractionCount++] = second;
                			if (isNumber(second)) {
                				
                				pos++;
                				length += 2;
                				sourcePos += 2;
                				
                				break;
                    		}
                    	}
					    final int insertLengthNumericCapture = sourceLength-length;
                        assert(insertLengthNumericCapture>=1);
                        
                    	//this is not a number we are inserting so it goes on the end.
						writeEnd(writeRuns(appendAltBranch(pos-1, source, sourcePos, insertLengthNumericCapture, sourceMask), source, sourcePos, insertLengthNumericCapture, sourceMask), value);
					    return;

                    case TYPE_VALUE_BYTES:
                        fieldExtractionsCount++;       
                        maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                        
                        extractions[extractionCount++] = ESCAPE_CMD_BYTES;
                                               
                    	if (ESCAPE_BYTE!=source[sourceMask & sourcePos]     || 
                    		ESCAPE_CMD_BYTES!=source[sourceMask & (sourcePos+1)] ||
                    		data[pos]!=source[sourceMask & (sourcePos+2)] ) {   
                       		////
                    		//this insert puts the new data on the very end instead of inserting it
                    		//in order to ensure the explicit patterns are always found "to the right"
                    		////
                    		final int insertLengthBytesCapture = sourceLength-length;								
							assert(insertLengthBytesCapture>=1);
							
							writeEnd(writeRuns(appendAltBranch(pos-1, source, sourcePos, insertLengthBytesCapture, sourceMask), source, sourcePos, insertLengthBytesCapture, sourceMask), value);
							
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
                        final int run = data[runPos];
                              
                        int r = run;
                        final int afterWhileRun    = run-(sourceLength-length);
                        if (sourceLength < run+length) {
                            
                            r = sourceLength-length;
                            assert(r<run);
                            int afterWhileLength = length+r;
                            while (--r >= 0) {
                                byte sourceByte = source[sourceMask & sourcePos++];
                                
                                //found an escape byte, so this set may need to break the run up.
                                if (ESCAPE_BYTE == sourceByte && NO_ESCAPE_SUPPORT!=ESCAPE_BYTE) {
                                    sourceByte = source[sourceMask & sourcePos++];
             
                                    //confirm second value is not also the escape byte so we do have a command
                                    if (ESCAPE_BYTE != sourceByte) {
                                        fieldExtractionsCount++;
                                                                                    
										insertAtBranchValueAlt(pos, source, sourceLength, sourceMask, value, length, runPos, run, r+afterWhileRun,	sourcePos-2); //TODO: this count can be off by buried extractions.      
									                                   
                                        maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                                        return;
                                    } else {
                                    	
                                       sourcePos+=1;//found literal
                                    }
                                    //else we have two escapes in a row therefore this is a literal
                                }                                
                                
                                if (data[pos++] != sourceByte) {
                                	                                	
                                    insertAtBranchValueByte(pos, source, sourceLength, sourceMask, value, length, runPos, run, r+afterWhileRun, sourcePos-1);    		
					
                                    maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                                    return;
                                }
                            }
                            length = afterWhileLength;
                            //matched up to this point but this was shorter than the run so insert a safe point
                            insertNewSafePoint(pos, source, sourcePos, afterWhileRun, sourceMask, value, runPos);     
                            maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                            return;
                        }                        
                        
                        while (--r >= 0) {
                            
                            byte sourceByte = source[sourceMask & sourcePos++];                            
                            
                            if ((data[pos] != sourceByte) && ESCAPE_BYTE == sourceByte && NO_ESCAPE_SUPPORT!=ESCAPE_BYTE) {
                            	//source byte is %
                                                
                                sourceByte = source[sourceMask & sourcePos++];
                                
                                if (ESCAPE_BYTE != sourceByte) {
                                    //sourceByte holds the specific command
                                    fieldExtractionsCount++;
                                    
									insertAtBranchValueAlt(pos, source, sourceLength, sourceMask, value, length, runPos, run, r, sourcePos-2);
                 
                                    maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                                    return;
                                } else {
                                
                                	//this was %%
                                    sourcePos+=1; //found literal
                                }
                                //else we have two escapes in a row therefore this is a literal
                            }                          
                            
                            if (data[pos++] != sourceByte) {
                                           	
                            	insertAtBranchValueByte(pos, source, sourceLength, sourceMask, value, length, runPos, run, r, sourcePos-1);    		
                            	                            	 
                                maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                                return;
                            }
                        }
                        
                        length+=run;

                        break;
                    case TYPE_END:
                    	
                        if (sourceLength>length) {
                            convertEndToNewSafePoint(pos, source, sourcePos, sourceLength-length, sourceMask, value);               
                        } else {
                            writeEndValue(pos, value);
                        }
                        maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount); //TODO: should this only be for the normal end??
                        return;
                        
                        
                    case TYPE_SAFE_END:
                    	
                    	
                        if (sourceLength>length) {
                            ///jump over the safe end values and continue on
                            pos += SIZE_OF_RESULT;
                            break;                            
                        } else {
          
                            pos = writeEndValue(pos, value);
                            maxExtractedFields = Math.max(maxExtractedFields, fieldExtractionsCount);
                            return;
                        }
                    default:
                    	logger.info("unknown op {}",this);
                        throw new UnsupportedOperationException("unknown op "+type+" at "+(pos-1));
                }
               
            }
        } else {
            //Start case where we insert the first run;
            pos = writeRuns( pos, source, sourcePos, sourceLength, sourceMask);
            limit = Math.max(limit, writeEnd(pos, value));
        }
        
        
    }

	private int choseOptimalPathFromStack(int[] choices) {
		int i = altStackPos;
		int selectedRank = Integer.MAX_VALUE;
		int selectedStackPos = -1;
		
		while (--i>=0) {
			final int type2 = (0xFF & data[altStackA[i]]);
			int c = choices.length;
			while (--c>=0) {
				if (type2 == choices[c]) {
					if (c < selectedRank) {
						selectedRank = c;
						selectedStackPos = i;
					}
					break;
				}
			}                        	
		}
		return selectedStackPos;
	}

	private int[] selectDataSpecificOrderedChoices(byte[] source, int sourcePos, int sourceMask) {
		int[] choices = null;
		if (ESCAPE_BYTE==source[sourceMask & sourcePos]) { 
			byte second = source[sourceMask & (sourcePos+1)];
			if ('b'==second) {
				choices = captureBytesChoices;
			} else {
				if (isNumber(second)) {
					choices = captureNumberChoices;
				} else {
					choices = definedChoices;
				}
			}
		} else {
			choices = definedChoices;
		}
		return choices;
	}

	private boolean isNumber(byte second) {
		return ESCAPE_CMD_UNSIGNED_INT==second || ESCAPE_CMD_UNSIGNED_HEX==second ||
			ESCAPE_CMD_DECIMAL==second      || ESCAPE_CMD_RATIONAL==second ||
					ESCAPE_CMD_OPTIONAL_SIGNED_INT==second ||
			ESCAPE_CMD_SIGNED_INT==second   || ESCAPE_CMD_SIGNED_HEX==second;
	}

	
    private int computeMax(byte[] source, int pos, int len, int mask) {
    	//int values can be long and we follow the same limits as the parser

    
    	int total = 0;
    	int i = len;
    	boolean escapeDetected = false;
    	while (--i>=0) {
    		
    		byte value = source[mask & pos++];
    		
    	    if (ESCAPE_BYTE == value) {
    	    	//if we have escape we turn it off, if off we turn it on
    	    	escapeDetected = !escapeDetected;
    	    	if (!escapeDetected) {
    	    		total++;
    	    	}
    	    } else {
    	    	if (escapeDetected) {
	    	    	
	    	    	if (ESCAPE_CMD_BYTES == value) {
	    	    		total += maxBytesCapturable;
	    	    	} else if (ESCAPE_CMD_DECIMAL == value ||
	    	    			   ESCAPE_CMD_RATIONAL == value ||
	    	    			   ESCAPE_CMD_SIGNED_INT == value ||
	    	    			   ESCAPE_CMD_SIGNED_HEX == value ||
	    	    			   ESCAPE_CMD_OPTIONAL_SIGNED_INT == value ||
	    	    			   ESCAPE_CMD_UNSIGNED_INT == value ||
	    	    			   ESCAPE_CMD_UNSIGNED_HEX == value
	    	    			) {
	    	    		total += maxNumericLenCapturable;
                    	} else {
	    	    		total++;
	    	    	}
    	    	} else {
    	    		total++;
    	    	}
    	    }
    	}
    	return total;
	}
    
	public void setMaxBytesCapturable(int value) {
		maxBytesCapturable = value;
	}
	
    public void setMaxNumericLengthCapturable(int value) {
    	maxNumericLenCapturable = value;
	}

	void recurseAltBranch(int pos, int offset) {
        if (data[pos] == TrieParser.TYPE_ALT_BRANCH) {
            pos++;
            assert(data[pos]>=0): "bad value "+data[pos];
            assert(data[pos+1]>=0): "bad value "+data[pos+1];
                
            altBranch( pos, offset, (((int)data[pos++])<<15) | (0x7FFF&data[pos++]), data[pos]);         
            
        } else {            
            pushAlt(pos, offset);           
        }
    }
    
    void altBranch(int pos, int offset, int jump, int peekNextType) {
        assert(jump>0) : "Jump must be postitive but found "+jump;
        
        //put extract first so its at the bottom of the stack
        if (TrieParser.TYPE_VALUE_BYTES == peekNextType 
        || TrieParser.TYPE_VALUE_NUMERIC==peekNextType
        || TrieParser.TYPE_ALT_BRANCH==peekNextType) {
            //Take the Jump value first, the local value has an extraction.
            //push the LocalValue        	
        	recurseAltBranch(pos+ TrieParser.BRANCH_JUMP_SIZE, offset);
        	recurseAltBranch(pos+jump+ TrieParser.BRANCH_JUMP_SIZE, offset); 
            
        } else {
            //Take the Local value first
            //push the JumpValue
        	recurseAltBranch(pos+jump+ TrieParser.BRANCH_JUMP_SIZE, offset);
        	recurseAltBranch(pos+ TrieParser.BRANCH_JUMP_SIZE, offset);
        	
           
        }
    }
    
    
    private void pushAlt(int pos, int sourcePos) {
        altStackA[altStackPos] = pos;
        altStackB[altStackPos++] = sourcePos;

    }

    static short buildNumberBits(byte sourceByte) { 
        
        switch(sourceByte) {
        	case ESCAPE_CMD_OPTIONAL_SIGNED_INT:
        	    return (short)(TrieParser.NUMERIC_FLAG_SIGN | TrieParser.NUMERIC_FLAG_ABSENT_IS_ZERO);        
            case ESCAPE_CMD_SIGNED_INT:
                return TrieParser.NUMERIC_FLAG_SIGN;
            case ESCAPE_CMD_UNSIGNED_INT:
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


    private void convertEndToNewSafePoint(int pos, byte[] source, int sourcePos, int sourceLength, int sourceMask, long value) {
        //convert end to safe, pos is now at the location of SIZE_OF_RESULT data
        
        if (data[pos-1] != TYPE_END) {
            throw new UnsupportedOperationException();
        }
        data[--pos] = TYPE_SAFE_END; //change to a safe and move pos back to beginning of this.

        //now insert the needed run 
        int requiredRoom = SIZE_OF_END_1 + sourceLength + midRunEscapeValuesSizeAdjustment(source, sourcePos, sourceLength, sourceMask);             
      
		makeRoomForInsert(0, pos, requiredRoom); //after the safe point we make room for our new run and end
		pos += SIZE_OF_SAFE_END;

        pos = writeRuns(pos, source, sourcePos, sourceLength, sourceMask);        
        pos = writeEnd(pos, value);

    }

    /**
     * Compute the additional space needed for any value extraction meta command found in the middle of a run.
     */
    private int midRunEscapeValuesSizeAdjustment(byte[] source, int sourcePos, int sourceLength, int sourceMask) {
        
        if (0==sourceLength) {
            return 0;
        }
        
       // new Exception(Appendables.appendUTF8(new StringBuilder("hello "), source, sourcePos, sourceLength, sourceMask).toString()).printStackTrace();
        
        int adjustment = 0;
        boolean needsRunStart = true;

        for(int i=0;i<sourceLength;i++) {
            byte value = source[sourceMask & (sourcePos+i)];
       
           // System.err.println(i+" "+((char)value));
            
            if (ESCAPE_BYTE == value && NO_ESCAPE_SUPPORT!=ESCAPE_BYTE) {
                i++;
                value = source[sourceMask & (sourcePos+i)];
                if (ESCAPE_BYTE != value) {
                    if (ESCAPE_CMD_BYTES == value) { //%bX
                    	if ((sourceLength>2) && (i<sourceLength-1)) {//do not adjust if %b was found at the end.
                    		i++;
                    		adjustment--; //bytes is 2 but to request it is 3 so go down by one
                    	}
                    }
                    
                    needsRunStart = true;
                    
                    
                    //NOTE: in many cases this ends up creating 1 extra!!!!
                    
                } else {
                	System.err.println("hello");
                	
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


    private int insertNewSafePoint(int pos, byte[] source, int sourcePos, int sourceLength, int sourceMask, long value, int runLenPos) {
        //convert end to safe
        
        makeRoomForInsert(sourceLength, pos, SIZE_OF_SAFE_END);
        
        data[pos++] = TYPE_SAFE_END;
        pos = writeEndValue(pos, value);

        pos = writeRunHeader(pos, sourceLength);
        data[runLenPos] -= sourceLength;//previous run is shortened buy the length of this new run
        return pos;
    }

	private void insertAtBranchValueAlt(final int pos, byte[] source, int sourceLength, int sourceMask,
			long value, int length, int runPos, int run, int r1, final int sourceCharPos) {
		r1++;
		if (r1 == run) {
			
			final int insertLength = sourceLength - length;
			assert(insertLength>=1);
		           
			writeEnd(writeRuns(insertAltBranch(0, pos>=3 ? pos-3 : 0, source, sourceCharPos, insertLength, sourceMask), source, sourceCharPos, insertLength, sourceMask), value);

		} else {
			
			final int insertLength = sourceLength - (length+(data[runPos] = (short)(run-r1)));
			assert(insertLength>=1);
			           
			writeEnd(writeRuns(insertAltBranch(r1, pos, source, sourceCharPos, insertLength, sourceMask), source, sourceCharPos, insertLength, sourceMask), value);
		}
	}

	private void insertAtBranchValueByte(final int pos, byte[] source, int sourceLength, int sourceMask,
			long value, int length, int runPos, int run, int r1, final int sourceCharPos) {
		r1++;
		if (r1 == run) {
			final int sourceLength1 = sourceLength - length;
			assert(sourceLength1>=1);
			writeEnd(writeRuns(insertByteBranch(0, pos>=3 ? pos-3 : 0, source, sourceCharPos, sourceLength1, sourceMask), source, sourceCharPos, sourceLength1, sourceMask), value);
		} else {
			final int sourceLength1 = sourceLength - (length+(data[runPos] = (short)(run-r1)));
			assert(sourceLength1>=1);

			writeEnd(writeRuns(insertByteBranch(r1, pos-1, source, sourceCharPos, sourceLength1, sourceMask), source, sourceCharPos, sourceLength1, sourceMask), value);
		}
	}





	private int insertByteBranch(int danglingByteCount, int pos, byte[] source, final int sourcePos, final int sourceLength, int sourceMask) {
		final int requiredRoom = SIZE_OF_END_1 + SIZE_OF_BRANCH + sourceLength + midRunEscapeValuesSizeAdjustment(source, sourcePos, sourceLength, sourceMask);
		            		
		final int oldValueIdx = makeRoomForInsert(danglingByteCount, pos, requiredRoom);
		byte c = source[sourcePos & sourceMask];
		pos = writeBranch(TYPE_BRANCH_VALUE, pos, requiredRoom, findSingleBitMask((short) c, this.data[oldValueIdx]));
		return pos;
	}

	/**
	 * Inserts run with end at this point. Moves data down to make room for this run.
	 * The alternate case will jump over this run and continue as normal.
	 * 
	 * @param danglingByteCount
	 * @param pos
	 * @param source
	 * @param sourcePos
	 * @param sourceLength
	 * @param sourceMask
	 */
	private int insertAltBranch(int danglingByteCount, int pos, byte[] source, final int sourcePos, final int sourceLength, int sourceMask) {

		int requiredRoom = SIZE_OF_END_1 + SIZE_OF_ALT_BRANCH + sourceLength + midRunEscapeValuesSizeAdjustment(source, sourcePos, sourceLength, sourceMask);  

		makeRoomForInsert(danglingByteCount, pos, requiredRoom);

		requiredRoom -= SIZE_OF_ALT_BRANCH;//subtract the size of the branch operator
		data[pos++] = TYPE_ALT_BRANCH;         
		             
		data[pos++] = (short)(0x7FFF&(requiredRoom>>15));          
		data[pos++] = (short)(0x7FFF&requiredRoom);
		return pos;
	}

	//need insert all which keeps the normal run and jumps all the way to the end where we will put the new run.
	
	private int appendAltBranch(int pos, byte[] source, final int sourcePos, final int sourceLength, int sourceMask) {

		final int requiredRoom = SIZE_OF_ALT_BRANCH;
		final int toBeMoved = limit - pos;

		limit+=requiredRoom;      
		int roomToGrow = SIZE_OF_END_1 + sourceLength + midRunEscapeValuesSizeAdjustment(source, sourcePos, sourceLength, sourceMask);  
	
		if (limit+toBeMoved+roomToGrow > data.length) {
			growDataLen(limit+toBeMoved+roomToGrow);        	
		}       
		
		if (toBeMoved <= 0) {
		    //nothing to be moved
		} else {
		    updatePreviousJumpDistances(0, data, pos, requiredRoom);
		    assert(pos>=0);
		    System.arraycopy(data, pos, data, pos + requiredRoom, toBeMoved);
		}
		
		data[pos++] = TYPE_ALT_BRANCH;         
		//this will jump to the end, we are assuming that some later call will add something to that point
		data[pos++] = (short)(0x7FFF&(toBeMoved>>15));          
		data[pos++] = (short)(0x7FFF&toBeMoved);
		
		int target = limit;
		limit +=roomToGrow;		
		return target;
	}
	
	

    private short findSingleBitMask(short a, short b) {
    	
    	
        int mask = 1<<5; //default of sign bit, only used when nothing replaces it. (critical for case insensitivity)       
        for(int i=0; i<8; i++) {            
            if (5!=i) { //sign bit, we do not use it unless all the others are tested first                
                int localMask = 1 << i;
                if ((localMask&a) != (localMask&b)) {
                	mask = localMask;
                    break;
                }
            }          
        }        
        
//        if (mask == (1<<5)) { ////////////////////////////only for debug tracking down case issues 
//        	System.err.println("ERROR HHHHHHHHHHHHHHHH  jump mask "+Integer.toBinaryString(mask)+" "+(char)a+" vs "+(char)b+"   "+a+" vs "+b);
//        	new Exception().printStackTrace();
//        }
        short result =  (short)(( 0xFF00&((mask&b)-1) ) | mask); //high byte is on when A matches mask
        assert((result&a) != (result&b));        
        return result;
    }

    private int makeRoomForInsert(int danglingByteCount, int pos, int requiredRoom) {
                
    	
        final int toBeMoved = limit - pos;
        if (danglingByteCount > 0) {
            requiredRoom+=SIZE_OF_RUN; //added because we will prepend this with a TYPE_RUN header to close the dangling bytes
        }
        
        
        int neededLen = limit+requiredRoom+SIZE_OF_RUN;
        if (neededLen > data.length) {
        	growDataLen(neededLen);        	
        }
        
        limit+=requiredRoom;      
        int newPos;
        if (toBeMoved <= 0) {
            newPos = pos;//nothing to be moved
        } else {                

	        updatePreviousJumpDistances(0, data, pos, requiredRoom);        
	        
	        newPos = pos + requiredRoom;
	        assert(pos>=0);
	        
	        
	        System.arraycopy(data, pos, data, newPos, toBeMoved);
	        
	        if (danglingByteCount > 0) {//do the prepend because we now have room
	            data[newPos-2] = TYPE_RUN;
	            data[newPos-1] = (short)danglingByteCount;
	        } else {
	            //new position already has the start of run so move cursor up to the first data point 
	            newPos+=SIZE_OF_RUN;
	        }
        }
        return newPos;
    }

	private void growDataLen(int neededLen) {
		if (this.fixedSize) {
			throw new UnsupportedOperationException("allocated length of "+data.length+" is too short to add all the patterns");
		} else {
			int newLen = data.length*2;
			if (newLen < neededLen) {
				newLen = neededLen;
			}       		
			
			short[] newData = new short[newLen];
			System.arraycopy(data, 0, newData, 0, data.length);        		
			data = newData;
			
		}
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
                	logger.info("unknown op {}",this);
                    throw new UnsupportedOperationException("ERROR Unrecognized value "+data[i]+" at "+i);
            }            
        }
    }


    private int writeBranch(byte type, int pos, int requiredRoom, short criteria) {
                
        int neededLen = pos+4;
        if (neededLen>data.length) {
     	   growDataLen(neededLen); 
        }
    	
        requiredRoom -= SIZE_OF_BRANCH;//subtract the size of the branch operator
        data[pos++] = type;
        data[pos++] = criteria;
        
        data[pos++] = (short)(0x7FFF&(requiredRoom>>15));
        data[pos++] = (short)(0x7FFF&requiredRoom);

        return pos;
    }


    private int writeEnd(int pos, long value) {
        int neededLen = pos+1+SIZE_OF_RESULT;
        if (neededLen>data.length) {
     	   growDataLen(neededLen); 
        }
        
        data[pos++] = TYPE_END;
        return writeEndValue(pos, value);
    }


    private int writeEndValue(int pos, long value) {
        
        int s = SIZE_OF_RESULT;
        while (--s >= 0) {        
            data[pos++] = (short)(0xFFFF& (value>>(s<<4)));        
        }
        return pos;
    }
    
    static long readEndValue(short[] data, int pos, int resultSize) {
        
    	if (resultSize<=2) {
    		if (resultSize == 2) {
    			//2 -- most common choice
    			return (((int)data[pos])<<16) | (0xFFFFL & data[1+pos]);
    		} else {
    			//1
    			return data[pos];
    		}
    	} else {
    		if (resultSize == 3) {
    			//3
    			return (((long)data[pos])<<32) 
    					| ((0xFFFFL & data[1+pos])<<16) 
    					| (0xFFFFL & data[2+pos]);
    		} else {
    			//4
      			return (((long)data[pos])<<48) 
      					| ((0xFFFFL & data[1+pos])<<32) 
    					| ((0xFFFFL & data[2+pos])<<16) 
    					| (0xFFFFL & data[3+pos]);
    			
    		}    		
    	}
    	
    }
 
    private int writeBytesExtract(int pos, short stop) {
    	        
		int neededLen = pos+2;
		if (neededLen>data.length) {
			   growDataLen(neededLen); 
		}

        data[pos++] = TYPE_VALUE_BYTES;
        data[pos++] = stop;
        extractions[extractionCount++] = ESCAPE_CMD_BYTES;
        return pos;
    }
    
    private int writeNumericExtract(int pos, int type) {
    	
		int neededLen = pos+2;
		if (neededLen>data.length) {
			   growDataLen(neededLen); 
		}
		
        data[pos++] = TYPE_VALUE_NUMERIC;
        data[pos++] = buildNumberBits((byte)type);
        extractions[extractionCount++] = (byte)type;
        return pos;
    }
 
    private int writeRuns(int pos, byte[] source, int sourcePos, int sourceLength, int sourceMask) {
       if (sourceLength<=0) {
           return pos;
       }
       
       //check for room first.
       int neededLen = pos+sourceLength+SIZE_OF_RUN;
       if (neededLen>data.length) {
    	   growDataLen(neededLen); 
       }
       
       assert(ESCAPE_BYTE != source[sourceMask & (sourcePos+sourceLength-1)]) : "Escape byte is always followed by something and can not be last.";
              
       pos = writeRunHeader(pos, sourceLength);
       int runLenPos = pos-1;
       int runLeft = sourceLength;
       int sourceStop = sourceLength+sourcePos;
       short activeRunLength = 0;
       while (--runLeft >= 0) {
                  byte value = source[sourceMask & sourcePos++];
                  if (ESCAPE_BYTE == value && NO_ESCAPE_SUPPORT!=ESCAPE_BYTE) {
                     
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
                              byte stop = sourcePos<sourceStop ? source[sourceMask & sourcePos++] : 0; //end of run if this is the end of the template pattern
                              pos = writeBytesExtract(pos, stop);
                              
                              //Recursion used to complete the rest of the run.
                              int remainingLength = runLeft-2;
                              if (remainingLength > 0) {
                                  pos = writeRuns(pos, source, sourcePos, remainingLength, sourceMask);
                              }
                          } else {
                              pos = writeNumericExtract(pos, value);                                                            
                              int remainingLength = runLeft-1;                                                         
                              if (remainingLength > 0) {
                                  pos = writeRuns(pos, source, sourcePos, remainingLength, sourceMask);
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

    private int writeRunHeader(int pos, int sourceLength) {
        
		int neededLen = pos+2;
		if (neededLen>data.length) {
			   growDataLen(neededLen); 
		}
    	
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

	public void toDOTFile(File targetFile) {
		
		try {
			String filename = targetFile.getAbsolutePath();
			System.out.println("dot -Tsvg -o"+filename+".svg "+filename);
						
			PrintStream printStream = new PrintStream(targetFile);
			toDOT(printStream);
			printStream.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}



    
}
