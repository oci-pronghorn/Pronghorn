package com.ociweb.jfast.ring;

import java.util.Arrays;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.generator.GeneratorUtils;

public class FieldReferenceOffsetManager {
	
	public static final int SEQ     = 0x10000000;
	public static final int MSG_END = 0x80000000;
	
    public int preambleOffset; //-1 if there is no preamble
    public int templateOffset;
    
    public int tokensLen;
    public int[] fragDataSize;
    public int[] fragScriptSize;
    public int[] tokens;
    public int[] starts; //TODO: make templateID in message the same as the cursor so this lookup will no longer be needed.

    public String[] fieldNameScript;
    public int maximumFragmentStackDepth;  
    
    private static int[] SINGLE_MESSAGE_BYTEARRAY_TOKENS = new int[]{TokenBuilder.buildToken(TypeMask.ByteArray, 
														            OperatorMask.Field_None, 
														            0)};
	private static int[] SINGLE_MESSAGE_BYTEARRAY_STARTS = new int[]{0};//there is only 1 message and only one start location.
	private static String[] SINGLE_MESSAGE_BYTEARRAY_NAMES = new String[]{"ByteArray"};
	private static final short ZERO_PREMABLE = 0;
	public static final FieldReferenceOffsetManager RAW_BYTES = new FieldReferenceOffsetManager(SINGLE_MESSAGE_BYTEARRAY_TOKENS, 
			                                                                                    ZERO_PREMABLE, 
			                                                                                    SINGLE_MESSAGE_BYTEARRAY_STARTS, 
			                                                                                    SINGLE_MESSAGE_BYTEARRAY_NAMES);
    /**
     * Constructor is only for unit tests.
     */
    private FieldReferenceOffsetManager() {    	
    	this(SINGLE_MESSAGE_BYTEARRAY_TOKENS, ZERO_PREMABLE, SINGLE_MESSAGE_BYTEARRAY_STARTS, SINGLE_MESSAGE_BYTEARRAY_NAMES);
    }

    //NOTE: message fragments start at startsLocal values however they end when they hit end of group, sequence length or end the the array.
    
	public FieldReferenceOffsetManager(int[] scriptTokens, short preableBytes, int[] startsLocal, String[] fieldNameScriptLocal) {
		//TODO: B, clientConfig must be able to skip reading the preamble,
        int PREAMBLE_MASK = 0xFFFFFFFF;//Set to zero when we are not sending the preamble
        
		int pb = PREAMBLE_MASK & preableBytes;
        if (pb<=0) {
            preambleOffset = -1;
            templateOffset = 0;
        } else {
            preambleOffset = 0;
            templateOffset = (pb+3)>>2;
        }
         
		if (null == scriptTokens) {
            fragDataSize = null;
            fragScriptSize = null;
            maximumFragmentStackDepth = 0;
        } else {
        
            fragDataSize  = new int[scriptTokens.length]; //size of fragments and offsets to fields, first field of each fragment need not use this!
            fragScriptSize = new int[scriptTokens.length];
            //TODO: D, could be optimized after the fragments are given the expected locations, for now this works fine.
            maximumFragmentStackDepth = scriptTokens.length;
            			
			buildFragScript(scriptTokens, preableBytes);
        }
        tokens = scriptTokens;
        tokensLen = null==tokens?0:tokens.length;
        
        starts = startsLocal;
        
        fieldNameScript = fieldNameScriptLocal;
	}

    private void buildFragScript(int[] scriptTokens, short preableBytes) {
		int scriptLength = scriptTokens.length;        
        boolean debug = false;       
        int i = 0;      
        int fragmentStartIdx=0;
        
        int depth = 0; //need script jump number
        
        boolean nextTokenOpensFragment = false;
        
        while (i<scriptLength) {            
            //now past the end of the template so 
            //close it because this index starts a new one
            //first position is always part of a new template
            
            //sequences and optional groups will always have group tags.
            int type = TokenBuilder.extractType(scriptTokens[i]);
            boolean isGroup = TypeMask.Group == type;    
            boolean isGroupOpen = isGroup && (0 == (scriptTokens[i] & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isGroupClosed = isGroup && (0 != (scriptTokens[i] & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isSeqLength = TypeMask.GroupLength == type;
                      
            if (isGroupOpen || nextTokenOpensFragment) {
                if (debug) {
                    System.err.println();
                }
                depth++;                
                fragmentStartIdx = i;       
                
                boolean isSeq = (0 != (scriptTokens[i] & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER)));
                //TODO: if optional group it will also need to be zero like seq
                
                //must be a group open only for a new message 
                if (!isSeq && isGroupOpen) { 
					int preambleInts = (preableBytes+3)>>2;
                    int templateInt = 1;
                    fragDataSize[fragmentStartIdx] = preambleInts+templateInt;
                }
                
                
                nextTokenOpensFragment = false;
            }
            
            if (isGroupClosed) {
                depth--;
                nextTokenOpensFragment = true;
            }
            if (isSeqLength) {
                nextTokenOpensFragment = true;
            }
            
            int token = scriptTokens[i];
            
            fragDataSize[i]=fragDataSize[fragmentStartIdx]; //keep the individual offsets per field
            
            int fSize = TypeMask.ringBufferFieldSize[TokenBuilder.extractType(token)];
            if (!GeneratorUtils.WRITE_CONST && !TokenBuilder.isOptional(token) && TokenBuilder.extractOper(token)==OperatorMask.Field_Constant) {
                fSize = 0; //constants are not written
            }
            
            fragDataSize[fragmentStartIdx] += fSize;
            fragScriptSize[fragmentStartIdx]++;
            

            if (debug) {
                System.err.println(depth+"  "+i+"  "+TokenBuilder.tokenToString(scriptTokens[i]));
            }
            
            i++;
        }
                
        if (debug) {
            System.err.println(Arrays.toString(fragDataSize));
            System.err.println(Arrays.toString(fragScriptSize));
            
        }
	}
    
    
  //TODO: convert to static and swap position for field id.
    public final String fieldName(int fragmentStart, int position) {
    	return fieldNameScript[fragmentStart+position];
    }
    
    
    //TODO: convert to static
    public final int lookupIDX(String target, int framentStart) {
        int x = framentStart;
        
        int UPPER_BITS = 0xF0000000;
        //System.err.println("looking for "+target+ " between "+x+" and "+limit);
        //System.err.println(Arrays.toString(fieldNameScript));
        
        while (true) {
        	//System.err.println("looking at:"+fieldNameScript[x]);
            if (fieldNameScript[x].equalsIgnoreCase(target)) {
                
                if (0==x) {
                    return UPPER_BITS|0; //that slot does not hold offset but rather full fragment size but we know zero can be used here.
                } else {
                    //System.err.println("found at "+x);
                    //System.err.println(Arrays.toString(fragDataSize));
                    return UPPER_BITS|fragDataSize[x];                    
                }
                
            }
            
            int type = TokenBuilder.extractType(tokens[x]);
            boolean isGroup = TypeMask.Group == type;    
          //  boolean isGroupOpen = isGroup && (0 == (tokens[x] & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isGroupClosed = isGroup && (0 != (tokens[x] & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isSeqLength = TypeMask.GroupLength == type;
            
            if (isGroupClosed || isSeqLength) {
            	break;
            }
            
            x++;
        }
        throw new UnsupportedOperationException("Unable to find field name: "+target+" in "+Arrays.toString(fieldNameScript));
        
    }
    
    
}
