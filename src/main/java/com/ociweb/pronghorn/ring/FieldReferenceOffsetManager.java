package com.ociweb.pronghorn.ring;

import java.util.Arrays;

import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class FieldReferenceOffsetManager {
	
	public static final int SEQ     = 0x10000000;
	public static final int MSG_END = 0x80000000;
	
    public int preambleOffset; //-1 if there is no preamble
    public int templateOffset;
    
    public int tokensLen;
    public int[] fragDataSize;
    public int[] fragScriptSize;
    public int[] tokens;
    public int[] messageStarts;
    
    public String[] fieldNameScript;
    public int maximumFragmentStackDepth;  
    
    private static int[] SINGLE_MESSAGE_BYTEARRAY_TOKENS = new int[]{TokenBuilder.buildToken(TypeMask.ByteArray, 
														                                      OperatorMask.Field_None, 
														                                      0)};
	private static String[] SINGLE_MESSAGE_BYTEARRAY_NAMES = new String[]{"ByteArray"};
	private static final short ZERO_PREMABLE = 0;
	public static final FieldReferenceOffsetManager RAW_BYTES = new FieldReferenceOffsetManager(SINGLE_MESSAGE_BYTEARRAY_TOKENS, 
			                                                                                    ZERO_PREMABLE, 
			                                                                                    SINGLE_MESSAGE_BYTEARRAY_NAMES);
	private final static int[] EMPTY = new int[0];
	
    /**
     * Constructor is only for unit tests.
     */
    private FieldReferenceOffsetManager() {    	
    	this(SINGLE_MESSAGE_BYTEARRAY_TOKENS, ZERO_PREMABLE, SINGLE_MESSAGE_BYTEARRAY_NAMES);
    }

    public FieldReferenceOffsetManager(int[] scriptTokens, String[] scriptNames) {
    	this(scriptTokens,(short)0,scriptNames);
    }    
    
    //NOTE: message fragments start at startsLocal values however they end when they hit end of group, sequence length or end the the array.
	public FieldReferenceOffsetManager(int[] scriptTokens, short preableBytes, String[] scriptNames) {
				
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
			//Not convinced we should support this degenerate case (null script) but it does make some unit tests much easer to write.
            fragDataSize = null;
            fragScriptSize = null;
            maximumFragmentStackDepth = 0;
            tokens = EMPTY;
        } else {
        
            fragDataSize  = new int[scriptTokens.length]; //size of fragments and offsets to fields, first field of each fragment need not use this!
            
            fragScriptSize = new int[scriptTokens.length];
            //TODO: D, could be optimized after the fragments are given the expected locations, for now this works fine.
            maximumFragmentStackDepth = scriptTokens.length;
            			
			buildFragScript(scriptTokens, preableBytes);
			tokens = scriptTokens;
        }
        tokensLen = null==tokens?0:tokens.length;
        
        
        fieldNameScript = scriptNames;
        messageStarts = computeMessageStarts(); 
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
    
    
    public int[] messageStarts() {
    	return messageStarts;
    }
    
    
    private int[] computeMessageStarts() {
		int countOfNeededStarts = 1; //zero is always a start regardless of the token type found at that location
		int j = tokens.length;
		while (--j>0) { //do not process zero we have already counted it
			int token = tokens[j];			
			
			if (TypeMask.Group == TokenBuilder.extractType(token) ) {				
				int opMask = TokenBuilder.extractOper(token);
				if ((OperatorMask.Group_Bit_Close & opMask)==0 &&     //this is an OPENING group not a CLOSE
				    (OperatorMask.Group_Bit_Templ & opMask)!=0 ) {    //this is a special GROUP called a TEMPLATE
					
					countOfNeededStarts ++;					
					
				}
			}
		}
		
		int[] result = new int[countOfNeededStarts];
				
		j = tokens.length;
		while (--j>0) { //do not process zero we have already counted it
			int token = tokens[j];			
			
			if (TypeMask.Group == TokenBuilder.extractType(token) ) {				
				int opMask = TokenBuilder.extractOper(token);
				if ((OperatorMask.Group_Bit_Close & opMask)==0 &&     //this is an OPENING group not a CLOSE
				    (OperatorMask.Group_Bit_Templ & opMask)!=0 ) {    //this is a special GROUP called a TEMPLATE
					
					result[--countOfNeededStarts] = j;							
					
				}
			}
		}
		result[--countOfNeededStarts] = 0;
		
		return result;
		
    }
    
    public static String lookupFieldName(int fragmentStart, int position, FieldReferenceOffsetManager from) {
		return from.fieldNameScript[fragmentStart+position];
	}
    
    /**
     * This does not return the token found in the script but rather a special value that can be used to 
     * get dead reckoning offset into the field location. 
     * 
     * @param target
     * @param framentStart
     * @param from
     * @return
     */
    public static int lookupFieldLocator(String target, int framentStart, FieldReferenceOffsetManager from) {
		int x = framentStart;
        
        int UPPER_BITS = 0xF0000000;
        //System.err.println("looking for "+target+ " between "+x+" and "+limit);
        //System.err.println(Arrays.toString(fieldNameScript));
        
        while (true) {
        	//System.err.println("looking at:"+fieldNameScript[x]);
            if (from.fieldNameScript[x].equalsIgnoreCase(target)) {
                
                if (0==x) {
                    return UPPER_BITS|0; //that slot does not hold offset but rather full fragment size but we know zero can be used here.
                } else {
                    //System.err.println("found at "+x);
                    //System.err.println(Arrays.toString(fragDataSize));
                    return UPPER_BITS|from.fragDataSize[x];                    
                }
                
            }
            
            int type = TokenBuilder.extractType(from.tokens[x]);
            boolean isGroup = TypeMask.Group == type;    
          //  boolean isGroupOpen = isGroup && (0 == (tokens[x] & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isGroupClosed = isGroup && (0 != (from.tokens[x] & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isSeqLength = TypeMask.GroupLength == type;
            
            if (isGroupClosed || isSeqLength) {
            	break;
            }
            
            x++;
        }
        throw new UnsupportedOperationException("Unable to find field name: "+target+" in "+Arrays.toString(from.fieldNameScript));
	}

    
    /**
     * Helpful debugging method that writes the script in a human readable form out to the console.
     * 
     * @param title
     * @param fullScript
     */
	public static void printScript(String title, FieldReferenceOffsetManager from) {
		System.out.println(title);
		int step = 3;
		
		String tab = "                                                 ";
		int i = 0;
		int depth = 3;
		while (i<from.tokens.length) {
			int token = from.tokens[i];
			
			if (TokenBuilder.extractType(token) ==  TypeMask.Group) {
				if ((TokenBuilder.extractOper(token)&OperatorMask.Group_Bit_Close)!=0 ) {
					depth-=step;
				}				
			}
			
			String row = "00000"+Integer.toString(i);
			
			String name = null!=from.fieldNameScript && i<from.fieldNameScript.length && null!=from.fieldNameScript[i] 
					        ? "   "+from.fieldNameScript[i] : "";
			
			System.out.println(row.substring(row.length()-6)+tab.substring(0,depth)+TokenBuilder.tokenToString(token)+name);		
			
			if (TokenBuilder.extractType(token) ==  TypeMask.Group) {
				if ((TokenBuilder.extractOper(token)&OperatorMask.Group_Bit_Close)==0 ) {
					depth+=step;
				} 				
			}
			i++;
		}
		
	}
    
	public int getAbsent32Value(int token) {
		//TODO: we should discuss what the default value for on the ring should be we have 4 choices
		return TokenBuilder.absentValue32(TokenBuilder.MASK_ABSENT_DEFAULT); //HACK until we add array lookup based on token id
	}
    
	public long getAbsent64Value(int token) {
		return TokenBuilder.absentValue64(TokenBuilder.MASK_ABSENT_DEFAULT); //HACK until we add array lookup based on token id
	}
    
}
