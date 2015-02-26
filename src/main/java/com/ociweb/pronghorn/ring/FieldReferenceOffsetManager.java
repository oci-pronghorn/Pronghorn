package com.ociweb.pronghorn.ring;

import java.util.Arrays;

import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class FieldReferenceOffsetManager {
	
	private static final String NAME_BYTE_ARRAY = "ByteArray";
	private static final String NAME_CHUNKED_STREAM = "Chunked Stream";
	
	public static final int SEQ     = 0x10000000;
	public static final int MSG_END = 0x80000000;
	
    public int preambleOffset; //-1 if there is no preamble
    public int templateOffset;
    
    public int tokensLen;
    public int[] fragDataSize;
    public int[] fragScriptSize;
    public int[] tokens;
    public int[] messageStarts;
    
    //NOTE: these two arrays could be combined with a mask to simplify this in the future.
    public int[] fragDepth;
    public int[] fragNeedsAppendedCountOfBytesConsumed;//TODO: put value in here to be indexed later when needed. low and high level will use it?
      //FAST decode looks it up at front of fragment and writes extra total filed
      //low level api will require message id to look up of trailing message is needed
      //high level api ??? how is this done?
    
    public final String[] fieldNameScript;
    public final long[] fieldIdScript;
    public final String[] dictionaryNameScript;
    public final int maximumFragmentStackDepth;  
    public final float maxVarFieldPerUnit;
    private int maxFragmentDataSize;
    private int minFragmentDataSize = Integer.MAX_VALUE;
    
    //TODO: B, set these upon construction if needed.
    //      for a given template/schema there is only 1 absent value that can be supported
	private final int absentInt = TokenBuilder.absentValue32(TokenBuilder.MASK_ABSENT_DEFAULT);
	private final long absentLong = TokenBuilder.absentValue64(TokenBuilder.MASK_ABSENT_DEFAULT); 
	
    
    private static int[] SINGLE_MESSAGE_BYTEARRAY_TOKENS = new int[]{TokenBuilder.buildToken(TypeMask.ByteArray, 
														                                      OperatorMask.Field_None, 
														                                      0)};
	private static String[] SINGLE_MESSAGE_BYTEARRAY_NAMES = new String[]{NAME_BYTE_ARRAY};
	private static long[] SINGLE_MESSAGE_BYTEARRAY_IDS = new long[]{0};
	private static final short ZERO_PREMABLE = 0;
	public static final FieldReferenceOffsetManager RAW_BYTES = new FieldReferenceOffsetManager(SINGLE_MESSAGE_BYTEARRAY_TOKENS, 
			                                                                                    ZERO_PREMABLE, 
			                                                                                    SINGLE_MESSAGE_BYTEARRAY_NAMES, 
			                                                                                    SINGLE_MESSAGE_BYTEARRAY_IDS,
			                                                                                    NAME_CHUNKED_STREAM);
		
	public static int LOC_CHUNKED_STREAM = 0;
	public static int LOC_CHUNKED_STREAM_FIELD = FieldReferenceOffsetManager.lookupFieldLocator(FieldReferenceOffsetManager.NAME_BYTE_ARRAY, LOC_CHUNKED_STREAM, FieldReferenceOffsetManager.RAW_BYTES);
	
	
	private final static int[] EMPTY = new int[0];
	public final String name;
	public final boolean hasSimpleMessagesOnly;
	
    private static final int STACK_OFF_BITS = 4; //Maximum stack depth of nested groups is 16, this can be increased if needed.
    
    public static final int RW_FIELD_OFF_BITS = (32-(STACK_OFF_BITS+TokenBuilder.BITS_TYPE));
    public final static int RW_STACK_OFF_MASK = (1<<STACK_OFF_BITS)-1;
    public final static int RW_STACK_OFF_SHIFT = 32-STACK_OFF_BITS;
    public final static int RW_FIELD_OFF_MASK = (1<<RW_FIELD_OFF_BITS)-1;
    
	
    /**
     * Constructor is only for unit tests.
     */
    private FieldReferenceOffsetManager() {    	
    	this(SINGLE_MESSAGE_BYTEARRAY_TOKENS, ZERO_PREMABLE, SINGLE_MESSAGE_BYTEARRAY_NAMES, SINGLE_MESSAGE_BYTEARRAY_IDS);
    }

    public FieldReferenceOffsetManager(int[] scriptTokens, String[] scriptNames, long[] scriptIds) {
    	this(scriptTokens,(short)0,scriptNames,scriptIds);
    }    
    
    public FieldReferenceOffsetManager(int[] scriptTokens, short preableBytes, String[] scriptNames, long[] scriptIds) {
    	this(scriptTokens,preableBytes,scriptNames,scriptIds,(String)null);
    }
    
    public FieldReferenceOffsetManager(int[] scriptTokens, short preableBytes, String[] scriptNames, long[] scriptIds,  String name) {
    	this(scriptTokens,preableBytes,scriptNames,scriptIds, new String[scriptTokens.length], name);
    	//dictionary names provide a back channel to pass information that relates to template choices when decoding/encoding object    	
    }
    
    //NOTE: message fragments start at startsLocal values however they end when they hit end of group, sequence length or end the the array.
	public FieldReferenceOffsetManager(int[] scriptTokens, short preableBytes, String[] scriptNames, long[] scriptIds, String[] scriptDictionaryNames, String name) {
			
		this.name = name;
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
          
        dictionaryNameScript = scriptDictionaryNames;
        fieldNameScript = scriptNames;
        fieldIdScript = scriptIds;
        
		if (null == scriptTokens) {
			tokens = EMPTY;
			messageStarts = computeMessageStarts(); 
			
			//Not convinced we should support this degenerate case (null script) but it does make some unit tests much easer to write.
            fragDataSize = null;
            fragScriptSize = null;
            fragDepth = null;
            fragNeedsAppendedCountOfBytesConsumed = new int[1];
            
            maximumFragmentStackDepth = 0;
            maxVarFieldPerUnit = .5f;  
            hasSimpleMessagesOnly = false; //unknown case so set false.
            
        } else {
        	tokens = scriptTokens;
        	messageStarts = computeMessageStarts(); 
        	 
            fragDataSize  = new int[scriptTokens.length]; //size of fragments and offsets to fields, first field of each fragment need not use this!
            fragScriptSize = new int[scriptTokens.length];
            fragDepth = new int[scriptTokens.length];
            fragNeedsAppendedCountOfBytesConsumed = new int[scriptTokens.length];//full of zeros by default
            
            maxVarFieldPerUnit = buildFragScript(scriptTokens, preableBytes);
            
            //walk all the depths to find the deepest point.
            int m = 0; 
            int i = fragDepth.length;
            
            while (--i>=0) {
            	m = Math.max(m, fragDepth[i]+1); //plus 1 because these are offsets and I want count
            }
            maximumFragmentStackDepth = m;
            
            //when the max depth is only one it is because there are no sub fagments found inside any messages
            hasSimpleMessagesOnly = (1==maximumFragmentStackDepth);
            			
            //consumer of this need not check for null because it is always created.
        }
        tokensLen = null==tokens?0:tokens.length;
          
	}

	public String toString() {
		if (null==name) {
			return fieldNameScript.length<20 ? Arrays.toString(fieldNameScript) : "ScriptLen:"+fieldNameScript.length;
		} else {
			return name;
		}
	}
	
	
	
    private float buildFragScript(int[] scriptTokens, short preableBytes) {
    	int spaceForTemplateId = 1;
		int scriptLength = scriptTokens.length;        
        boolean debug = false;       
        int i = 0;      
        int fragmentStartIdx=0;
        int depth = 0; //used for base jub location when using high level API.
        int sumOfVarLengthFields = 0;
        
        boolean nextTokenOpensFragment = true;// false; //must capture simple case when we do not have wrapping group?
        
        //must count these to ensure that var field writes stay small enough to never cause a problem in the byte ring.
        int varLenFieldCount = 0;
        int varLenFieldLast = 0;
        float varLenMaxDensity = 0; //max varLength fields per int on outer ring buffer than can ever happen
        
        //
        //in order to provide the byte length data for readers we must add a following int
        //for all fragments in order to capture the count of bytes that belong to this fragment
        //however this should not be added if there is only 1 var length field and it is the last one
        //this is because that is the value that will already appear in that slot.
        //
        
        
        while (i<scriptLength) {          
           	
            //now past the end of the template so 
            //close it because this index starts a new one
            //first position is always part of a new template
            
            //sequences and optional groups will always have group tags.
            int tempToken = scriptTokens[i];
        	int type = TokenBuilder.extractType(tempToken);
            boolean isGroup = TypeMask.Group == type;    
            boolean isGroupOpen = isGroup && (0 == (tempToken & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isGroupClosed = isGroup && (0 != (tempToken & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isSeqLength = TypeMask.GroupLength == type;
                      
            if (isGroupOpen || nextTokenOpensFragment) {
                if (debug) {
                    System.err.println();
                }
                //only save this at the end of each fragment, not on the first pass.
                if (i>fragmentStartIdx) {
                	//NOTE: this size can not be changed up without reason, any place the low level API is used it will need
                	//to know about the full size and append the right fields of the right size
                	accumVarLengthCounts(fragmentStartIdx, varLenFieldCount, varLenFieldLast);
                }
                
                int lastFragTotalSize = fragDataSize[fragmentStartIdx];
                maxFragmentDataSize = Math.max(maxFragmentDataSize, lastFragTotalSize);
                minFragmentDataSize = Math.min(minFragmentDataSize, lastFragTotalSize);
       //         System.err.println("new fragmetn at "+i);
                if (varLenFieldCount>0) {
                	//Caution: do not modify this logic unless you take into account the fact that
                	//         * messages are made up of fragments and that some fragments are repeated others skipped
                	//         * messages are not always complete and only some (head or tail) fragments may be in the buffer
                	float varFieldPerUnit = varLenFieldCount/ (float)lastFragTotalSize;
                	assert(varFieldPerUnit<=.5) : "It takes 2 units to write a var field so this will never be larger than .5";
                	
                	if (varFieldPerUnit>varLenMaxDensity) {
                		varLenMaxDensity = varFieldPerUnit;                		
                	}                	
                }
                
                fragmentStartIdx = i;    
                
                boolean isSeq = (0 != (scriptTokens[i] & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER)));
                //TODO: B, if optional group it will also need to be zero like seq
                
                fragDepth[fragmentStartIdx]= depth;//stack depth for reader and writer
                                
                
                
                //must be a group open only for a new message 
                if ((!isSeq && isGroupOpen) || (0==fragmentStartIdx)) { 
					int preambleInts = (preableBytes+3)>>2;     
                    assert(0==depth) : "check for length without following body, could be checked earlier in XML";
                    fragDataSize[fragmentStartIdx] = preambleInts+spaceForTemplateId;  //these are the starts of messages
                    
                    //System.err.println("started with ints at :"+fragmentStartIdx);
                    
                } else {
                	if (isGroupOpen) {
                		fragDataSize[fragmentStartIdx] = 0; //these are the starts of fragments that are not message starts
                		// System.err.println("started with zero at :"+fragmentStartIdx);
                	} else {
                		depth--;
                		fragDataSize[fragmentStartIdx] = -1; //these are group closings
                	}
                }
                depth++;                
                
                varLenFieldCount = 0;//reset to zero so we can count the number of var fields for this next fragment
                varLenFieldLast = 0;
                

                
                nextTokenOpensFragment = false;
            }
            
            int token = scriptTokens[i];
            int tokenType = TokenBuilder.extractType(token);

            if (isGroupClosed) {
                depth--;
                nextTokenOpensFragment = true;
            } else {
            	//do not count group closed against our search for if the last field is variable
            	varLenFieldCount += (varLenFieldLast=TypeMask.ringBufferFieldVarLen[tokenType]);
            }
            if (isSeqLength) {
                nextTokenOpensFragment = true;
            }
            
            
            fragDataSize[i]=fragDataSize[fragmentStartIdx]; //keep the individual offsets per field
            fragDepth[i] = fragDepth[fragmentStartIdx];
            
           // System.err.println("Token "+TokenBuilder.tokenToString(token));
            
            sumOfVarLengthFields += TypeMask.ringBufferFieldVarLen[tokenType];
            
			int fSize = TypeMask.ringBufferFieldSize[tokenType];
            
            fragDataSize[fragmentStartIdx] += fSize;
            fragScriptSize[fragmentStartIdx]++;
            

            if (debug) {
                System.err.println(depth+"  "+i+"  "+TokenBuilder.tokenToString(scriptTokens[i]));
            }
            
            i++;
        }
        
        accumVarLengthCounts(fragmentStartIdx, varLenFieldCount, varLenFieldLast);
        
        int lastFragTotalSize = fragDataSize[fragmentStartIdx];
        
        maxFragmentDataSize = Math.max(maxFragmentDataSize, lastFragTotalSize);
        minFragmentDataSize = Math.min(minFragmentDataSize, lastFragTotalSize);
        //must also add the very last fragment 
        if (varLenFieldCount>0) {
        	//Caution: do not modify this logic unless you take into account the fact that
        	//         * messages are made up of fragments and that some fragments are repeated others skipped
        	//         * messages are not always complete and only some (head or tail) fragments may be in the buffer
        	float varFieldPerUnit = varLenFieldCount/ (float)lastFragTotalSize;
        	assert(varFieldPerUnit<=.5) : "It takes 2 units to write a var field so this will never be larger than .5";
        	
        	if (varFieldPerUnit>varLenMaxDensity) {
        		varLenMaxDensity = varFieldPerUnit;                		
        	}                	
        }
        
                
        if (debug) {
            System.err.println(Arrays.toString(fragDataSize));
            System.err.println(Arrays.toString(fragScriptSize));
            
        }
        return varLenMaxDensity;
	}

	private void accumVarLengthCounts(int fragmentStartIdx,
			int varLenFieldCount, int varLenFieldLast) {

			//if last is 1 and count is 1 then don't else do			
			if (1!=varLenFieldCount || 1!=varLenFieldLast) {     
				fragDataSize[fragmentStartIdx]++;
				fragNeedsAppendedCountOfBytesConsumed[fragmentStartIdx] = 1; //in all other cases its zero.
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
		
		//System.err.println("the starts:"+Arrays.toString(result));
		
		return result;
		
    }
    
    public static int maxVarLenFieldsPerPrimaryRingSize(FieldReferenceOffsetManager from, int mx) {
		int maxVarCount = (int)Math.ceil(mx*from.maxVarFieldPerUnit);
		//we require at least 2 fields to ensure that the average approach works in all cases
		if (maxVarCount < 2) {
			// 2 = size * perUnit
			int minSize = (int)Math.ceil(2f/from.maxVarFieldPerUnit);
			int minBits = 32 - Integer.numberOfLeadingZeros(minSize - 1);
			throw new UnsupportedOperationException("primary buffer is too small it must be at least "+minBits+" bits"); 
		}
		return maxVarCount;
	}

	public static int lookupTemplateLocator(String name, FieldReferenceOffsetManager from) {
    	int i = from.messageStarts.length;
    	while(--i>=0) {
    		if (name.equals(from.fieldNameScript[from.messageStarts[i]])) {
    			
    			return from.messageStarts[i];
    		}
    	}
    	throw new UnsupportedOperationException("Unable to find template name: "+name);
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
        		
		//upper bits is 4 bits of information

        while (x < from.fieldNameScript.length) {
            if (from.fieldNameScript[x].equalsIgnoreCase(target)) {            	
            	return buildFieldLoc(from, framentStart, x);                
            }
            
            int token = from.tokens[x];
            int type = TokenBuilder.extractType(token);
            boolean isGroupClosed = TypeMask.Group == type &&
            		                (0 != (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER))) &&
            		                (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER)));
           
            if (isGroupClosed) {
            	break;
            }
            
            x++;
        }
        throw new UnsupportedOperationException("Unable to find field name: "+target+" in "+Arrays.toString(from.fieldNameScript));
	}

	private static int buildFieldLoc(FieldReferenceOffsetManager from,
			int framentStart, int fieldCursor) {
		final int stackOff = from.fragDepth[framentStart]<<RW_STACK_OFF_SHIFT;
		int fieldType = TokenBuilder.extractType(from.tokens[fieldCursor])<<RW_FIELD_OFF_BITS;
		//type is 5 bits of information
		
		//the remaining bits for the offset is 32 -(4+5) or 23 which is 8M for the fixed portion of any fragment
		
		int fieldOff =  (0==fieldCursor) ? from.templateOffset+1 : from.fragDataSize[fieldCursor];
		assert(fieldOff>=0);
		assert(fieldOff < (1<<RW_FIELD_OFF_BITS)) : "Fixed portion of a fragment can not be larger than "+(1<<RW_FIELD_OFF_BITS)+" bytes";
		
		return stackOff | fieldType | fieldOff;
	}

    public static int lookupToken(String target, int framentStart, FieldReferenceOffsetManager from) {
    	return from.tokens[lookupFragmentLocator(target,framentStart,from)];
    }
    
    public static int lookupSequenceLengthLoc(String target, int framentStart, FieldReferenceOffsetManager from) {
    	int x = lookupFragmentLocator(target, framentStart, from);
    	return buildFieldLoc(from, framentStart, x-1);
    }
	
	
    public static int lookupFragmentLocator(String target, int framentStart, FieldReferenceOffsetManager from) {
		int x = framentStart;
        		
        while (x < from.fieldNameScript.length) {
            if (from.fieldNameScript[x].equalsIgnoreCase(target)) {
            	return x;
            }
            
            int token = from.tokens[x];
            int type = TokenBuilder.extractType(token);
            boolean isGroupClosed = TypeMask.Group == type &&
            		                (0 != (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER))) &&
            		                (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER)));
           
            if (isGroupClosed) {
            	break;
            }
            
            x++;
        }
        throw new UnsupportedOperationException("Unable to find fragment name: "+target+" in "+Arrays.toString(from.fieldNameScript));
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
    
	//TODO: C, if this really really needs more specific values we may be able to give custom values per column #.
	public static int getAbsent32Value(FieldReferenceOffsetManager from) {
		return from.absentInt;
	}
	
	public static long getAbsent64Value(FieldReferenceOffsetManager from) {
		return from.absentLong;
	}

	public static int maxFragmentSize(FieldReferenceOffsetManager from) {
		return from.maxFragmentDataSize;
	}
	
	public static int minFragmentSize(FieldReferenceOffsetManager from) {
		return from.minFragmentDataSize;
	}

	//NOTE: we use a special mask because the 4th bit may be on or off depending on pmap usage
	private static int TEMPL_MASK =  (0x17 << TokenBuilder.SHIFT_OPER) |
			                         (TokenBuilder.MASK_TYPE << TokenBuilder.SHIFT_TYPE);
	private static int TEMPL_VALUE = (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER) | 
			                           (TypeMask.Group << TokenBuilder.SHIFT_TYPE);
		
	public static boolean isTemplateStart(FieldReferenceOffsetManager from, int cursorPosition) {
		//checks the shortcut hasSimpleMessagesOnly first before any complex logic
		return from.hasSimpleMessagesOnly || (cursorPosition<=0) || cursorPosition>=from.fragDepth.length || (0==from.fragDepth[cursorPosition]);
	}
    
}
