package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.pipe.util.RLESparseArray;
import com.ociweb.pronghorn.pipe.util.hash.MurmurHash;
import com.ociweb.pronghorn.util.Appendables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class FieldReferenceOffsetManager {
	
	public static final int SEQ     = 0x10000000;
	public static final int MSG_END = 0x80000000;
	
    public int preambleOffset; //-1 if there is no preamble
    public int templateOffset;
    
    public int tokensLen;
    public final int[] fragDataSize;  //derived from tokens, do not put in hash or equals
    public final int[] fragScriptSize;  //derived from tokens, do not put in hash or equals
    public final int[] tokens;
    public final int[] messageStarts;
    private final long[] longDefaults; 
    private int[] intDefaults;
    
    //NOTE: these two arrays could be combined with a mask to simplify this in the future.
    public int[] fragDepth; //derived from tokens, do not put in hash or equals
    
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
	
	private final static int[] EMPTY = new int[0];
	public final static long[] RLE_LONG_NOTHING = new long[]{2,2,0};
	public final static int[] RLE_INT_NOTHING = new int[]{2,2,0};
    	
	public final String name;
	public final boolean hasSimpleMessagesOnly;
	
	private final static Logger logger = LoggerFactory.getLogger(FieldReferenceOffsetManager.class);
	
	
	
	//Maximum stack depth of nested groups is 32.
	//NOTE: we also keep the top top bit as zero
	//NOTE: the top bit is always ZERO to confirm that LOC values do NOT look like tokens which have the high bit on.
    private static final int STACK_OFF_BITS = 6; 
    
    public static final int RW_FIELD_OFF_BITS = (32-(STACK_OFF_BITS+TokenBuilder.BITS_TYPE));
    public final static int RW_STACK_OFF_MASK = (1<<STACK_OFF_BITS)-1;
    public final static int RW_STACK_OFF_SHIFT = 32-STACK_OFF_BITS;
    public final static int RW_FIELD_OFF_MASK = (1<<RW_FIELD_OFF_BITS)-1;
    public final short preableBytes;
	
    
    private int[] guid = new int[8];


    

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
    
    public FieldReferenceOffsetManager(int[] scriptTokens, short preableBytes, String[] scriptNames, long[] scriptIds, String[] scriptDictionaryNames, String name) {        
        this(scriptTokens, preableBytes, scriptNames, scriptIds, scriptDictionaryNames, name, RLE_LONG_NOTHING, RLE_INT_NOTHING);
    }  
    
    //NOTE: message fragments start at startsLocal values however they end when they hit end of group, sequence length or end the the array.
	public FieldReferenceOffsetManager(int[] scriptTokens, short preableBytes, String[] scriptNames, long[] scriptIds, String[] scriptDictionaryNames, String name, long[] longDefaults, int[] intDefaults) {
	    //These are mostly zeros
	    this.longDefaults = longDefaults == RLE_LONG_NOTHING ? RLE_LONG_NOTHING : longDefaults;
	    this.intDefaults = intDefaults == RLE_INT_NOTHING ? RLE_INT_NOTHING : intDefaults;	    
	    
		this.preableBytes = preableBytes;
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
			
			//Not convinced we should support this degenerate case (null script) but it does make some unit tests much easier to write.
            fragDataSize = null;
            fragScriptSize = null;
            fragDepth = null;
            
            maximumFragmentStackDepth = 0;
            maxVarFieldPerUnit = .5f;  
            hasSimpleMessagesOnly = false; //unknown case so set false.
            
        } else {
        	tokens = scriptTokens;
        	messageStarts = computeMessageStarts(); 
        	 
            fragDataSize  = new int[scriptTokens.length]; //size of fragments and offsets to fields, first field of each fragment need not use this!
            fragScriptSize = new int[scriptTokens.length];
            fragDepth = new int[scriptTokens.length];
            
            maxVarFieldPerUnit = buildFragScript(scriptTokens, preableBytes);
            
            
           // logger.info("{} max var field per unit {} ",name, maxVarFieldPerUnit);
            
            
            //walk all the depths to find the deepest point.
            int m = 0; 
            int i = fragDepth.length;
            
            while (--i>=0) {
            	m = Math.max(m, fragDepth[i]+1); //plus 1 because these are offsets and I want count
            }
            if (m>3) {
            	logger.info("warning, pipes are allocating room for a stack depth of "+m);
            }
            maximumFragmentStackDepth = m;
            
            //when the max depth is only one it is because there are no sub fragments found inside any messages
            hasSimpleMessagesOnly = (1==maximumFragmentStackDepth);
            			
            //consumer of this need not check for null because it is always created.
        }
        tokensLen = null==tokens?0:tokens.length;
        populateGUID();  
	}

	private void populateGUID() {
	    
	    //int[] scriptTokens, short preableBytes, String[] scriptNames, long[] scriptIds, String[] scriptDictionaryNames, String name, long[] longDefaults, int[] intDefaults
	    
	    this.guid[0] = MurmurHash.hash32(tokens, 0, tokens.length,               314-579-0066); //Need help call OCI
	    this.guid[1] = MurmurHash.hash32(fieldIdScript, 0, fieldIdScript.length, 314-579-0066); //Need help call OCI
	    this.guid[2] = MurmurHash.hash32(fieldNameScript,                        314-579-0066); //Need help call OCI
	    this.guid[3] = preableBytes;
	    this.guid[4] = MurmurHash.hash32(dictionaryNameScript,                   314-579-0066); //Need help call OCI
	    this.guid[5] = MurmurHash.hash32(name,                                   314-579-0066); //Need help call OCI
	    this.guid[6] = MurmurHash.hash32(longDefaults, 0, longDefaults.length,   314-579-0066); //Need help call OCI
	    this.guid[7] = MurmurHash.hash32(intDefaults, 0, intDefaults.length,     314-579-0066); //Need help call OCI
        
    }
	
	public int[] cloneGUID() {
	    return this.guid.clone();
	}
	
	public void validateGUID(int[] GUID) {
	    if (!Arrays.equals(GUID, this.guid)) {
	        throw new UnsupportedOperationException("The GUID version of this schema FROM does not match value when built. May need to regenerate from source OR Pipe Schema does not match expected Stage Schema.");
	    }
	}

    public String toString() {
		if (null==name) {
			return fieldNameScript.length<20 ? Arrays.toString(fieldNameScript) : "ScriptLen:"+fieldNameScript.length;
		} else {
			return name;
		}
	}
	
    private final boolean debug = false;       
	
    private float buildFragScript(int[] scriptTokens, short preableBytes) {
    	int spaceForTemplateId = 1;
		int scriptLength = scriptTokens.length;        
        int i = 0;      
        int fragmentStartIdx=0;
        int depth = 0; //used for base jub location when using high level API.
        
        boolean nextTokenOpensFragment = true;// false; //must capture simple case when we do not have wrapping group?
        
        //must count these to ensure that var field writes stay small enough to never cause a problem in the byte ring.
        int varLenFieldCount = 0;
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

        	int tempToken = scriptTokens[i]; 
        	//valid tokens are always negative, script length may go on past the data.
 //TODO: disabled because we have some tests that fail           assert(tempToken<0) : "valid tokens are always negative";
        	
            //sequences and optional groups will always have group tags.

        	int type = TokenBuilder.extractType(tempToken);
            boolean isGroup = TypeMask.Group == type;    
            boolean isGroupOpen = isGroup && (0 == (tempToken & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isGroupClosed = isGroup && (0 != (tempToken & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isSeqLength = TypeMask.GroupLength == type;
                      
            if (isGroupOpen || nextTokenOpensFragment) {
                if (debug) {
                    System.err.println();
                }
                int lastFragTotalSize = 0;
                //only save this at the end of each fragment, not on the first pass.
                if (i>fragmentStartIdx) {
                	//NOTE: this size can not be changed up without reason, any place the low level API is used it will need
                	//to know about the full size and append the right fields of the right size
                	fragDataSize[fragmentStartIdx]++;//Add one for trailing byte count on end of every fragment

                	lastFragTotalSize = fragDataSize[fragmentStartIdx];
                	assert(lastFragTotalSize<(1<<20)) : "Fragments must be smaller than 1MB";
                	assert(lastFragTotalSize>0) : "All fragments must be 1 or larger";
                	
                	maxFragmentDataSize = Math.max(maxFragmentDataSize, lastFragTotalSize);
                	minFragmentDataSize = Math.min(minFragmentDataSize, lastFragTotalSize);
                }
                

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
                    assert(fragDataSize[fragmentStartIdx]<65536) : "Premable is way to big, consider a different design";
                    //System.err.println("started with ints at :"+fragmentStartIdx);
                    
                } else {
                	if (isGroupOpen) {
                		fragDataSize[fragmentStartIdx] = 0; //these are the starts of fragments that are not message starts
                		// System.err.println("started with zero at :"+fragmentStartIdx);
                	} else {
                		depth--;
                		fragDataSize[fragmentStartIdx] = 0;//leave as zero so we can start trailing fragments   -1; //these are group closings
                	}
                }
                depth++;                
                
                varLenFieldCount = 0;//reset to zero so we can count the number of var fields for this next fragment
                
                nextTokenOpensFragment = false;
            }
            
            int token = scriptTokens[i];
            int tokenType = TokenBuilder.extractType(token);

            if (isGroupClosed) {
                depth--;
                nextTokenOpensFragment = true;
            } else {
            	//do not count group closed against our search for if the last field is variable
            	varLenFieldCount += (TypeMask.ringBufferFieldVarLen[tokenType]);
            }
            if (isSeqLength) {
                nextTokenOpensFragment = true;
            }
            
            
            fragDataSize[i]=fragDataSize[fragmentStartIdx]; //keep the individual offsets per field
            fragDepth[i] = fragDepth[fragmentStartIdx];
            
            
			int fSize = TypeMask.ringBufferFieldSize[tokenType];
			
            fragDataSize[fragmentStartIdx] += fSize;
            fragScriptSize[fragmentStartIdx]++;
            

            if (debug) {
                System.err.println(depth+"  "+i+"  "+TokenBuilder.tokenToString(scriptTokens[i]));
            }
            
            i++;
        }
        
        fragDataSize[fragmentStartIdx]++;//Add one for trailing byte count on end of every fragment
        
        int lastFragTotalSize = fragDataSize[fragmentStartIdx];
        assert(lastFragTotalSize< (1<<30)) : "Fragments larger than this are possible but unlikely, You probably do not want to build this fragment of "+lastFragTotalSize;
        
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
            System.err.println("DataSize:"+Arrays.toString(fragDataSize));
            System.err.println("SrptSize:"+Arrays.toString(fragScriptSize));
            
        }
        return varLenMaxDensity;
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
    
    public static void debugFROM(FieldReferenceOffsetManager from) {
		int j = 0; ///debug code to be removed
		while (j<from.tokens.length) {
			System.err.println((j<10? " ": "" )+j+" Depth:"+from.fragDepth[j]+" ScrSiz:"+from.fragScriptSize[j]+ " DatSiz:"+from.fragDataSize[j]+" "+TokenBuilder.tokenToString(from.tokens[j]));
			j++;
		}
	}
    
    public static int extractTypeFromLoc(int fieldLoc) {
        assert(0==(fieldLoc>>31)) : "This is not a LOC";
        return (fieldLoc >> RW_FIELD_OFF_BITS ) & TokenBuilder.MASK_TYPE;

    }

	public static boolean isGroupSequence(FieldReferenceOffsetManager from, int cursor) {
		return 0 != (OperatorMask.Group_Bit_Seq&TokenBuilder.extractOper(from.tokens[cursor]));
	}

	public static boolean isGroupClosed(FieldReferenceOffsetManager from,  int cursor) {
		return isGroup(from, cursor) &&
		 0 != (OperatorMask.Group_Bit_Close&TokenBuilder.extractOper(from.tokens[cursor]));
	}

	public static boolean isGroupOpen(FieldReferenceOffsetManager from, int cursor) {
		return isGroup(from, cursor) && 
				0 == (OperatorMask.Group_Bit_Close&TokenBuilder.extractOper(from.tokens[cursor]));
	}
	
    public static boolean isGroupTemplate(FieldReferenceOffsetManager from, int cursor) {
        return isGroup(from, cursor) && 
                0 != (OperatorMask.Group_Bit_Templ&TokenBuilder.extractOper(from.tokens[cursor]));
    }

	public static boolean isGroup(FieldReferenceOffsetManager from, int cursor) {
		return TypeMask.Group == TokenBuilder.extractType(from.tokens[cursor]);
	}

	public static int maxVarLenFieldsPerPrimaryRingSize(FieldReferenceOffsetManager from, int mx) {
		if (0==from.maxVarFieldPerUnit) {
			return 0;
		}
		
		int maxVarCount = (int)Math.ceil((float)mx*from.maxVarFieldPerUnit);
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
    
	public static int lookupTemplateLocator(final long id, FieldReferenceOffsetManager from) {
    	int i = from.messageStarts.length;
    	while(--i>=0) {
    		if (id == from.fieldIdScript[from.messageStarts[i]]) {
    			
    			return from.messageStarts[i];
    		}
    	}
    	throw new UnsupportedOperationException("Unable to find template id: "+id);
    }
	
	public int getLoc(String messageName, String fieldName) {		
		return lookupFieldLocator(fieldName, lookupTemplateLocator(messageName, this), this);
	}

	public int getLoc(long messageId, long fieldId) {		
		return lookupFieldLocator(fieldId, lookupTemplateLocator(messageId, this), this);
	}
    
    /**
     * This does not return the token found in the script but rather a special value that can be used to 
     * get dead reckoning offset into the field location. 
     * 
     * @param name
     * @param fragmentStart
     * @param from
     */
    public static int lookupFieldLocator(String name, int fragmentStart, FieldReferenceOffsetManager from) {
		int x = fragmentStart;
        		
		//upper bits is 4 bits of information

        while (x < from.fieldNameScript.length) {
            if (name.equalsIgnoreCase(from.fieldNameScript[x])) {            	
            	return buildFieldLoc(from, fragmentStart, x);
            }
            
            if (exitSearch(from, x)) {
            	break;
            }
            
            x++;
        }
        throw new UnsupportedOperationException("Unable to find field name: "+name+" in "+Arrays.toString(from.fieldNameScript));
	}

    private static boolean exitSearch(FieldReferenceOffsetManager from, int x) {
        int token = from.tokens[x];
        int type = TokenBuilder.extractType(token);
        boolean isGroupClosed = TypeMask.Group == type &&
        		                (0 != (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER))) &&
        		                (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER)));
        return isGroupClosed;
    }

    public static int paranoidLookupFieldLocator(long id, String name, int fragmentStart, FieldReferenceOffsetManager from) {
		int x = fragmentStart;
        		
		//upper bits is 4 bits of information

        while (x < from.fieldNameScript.length) {
            if (id == from.fieldIdScript[x] && name.equalsIgnoreCase(from.fieldNameScript[x])) {            	
            	return buildFieldLoc(from, fragmentStart, x);
            }
            
            if (exitSearch(from, x)) {
            	break;
            }
            
            x++;
        }
        throw new UnsupportedOperationException("Unable to find field id: "+id+" in "+Arrays.toString(from.fieldNameScript));
	}
    
    public static int lookupFieldLocator(long id, int fragmentStart, FieldReferenceOffsetManager from) {
        int x = fragmentStart;
                
        //upper bits is 4 bits of information

        while (x < from.fieldNameScript.length) {
            if (id == from.fieldIdScript[x]) {              
                return buildFieldLoc(from, fragmentStart, x);
            }
            
            if (exitSearch(from, x)) {
                break;
            }
            
            x++;
        }
        throw new UnsupportedOperationException("Unable to find field id: "+id+" in "+Arrays.toString(from.fieldNameScript));
    }
    
	private static int buildFieldLoc(FieldReferenceOffsetManager from,
			int fragmentStart, int fieldCursor) {
		final int stackOff = from.fragDepth[fragmentStart]<<RW_STACK_OFF_SHIFT;
		final int shiftedFieldType = TokenBuilder.extractType(from.tokens[fieldCursor])<<RW_FIELD_OFF_BITS;
		//type is 5 bits of information
		
		//the remaining bits for the offset is 32 -(4+5) or 23 which is 8M for the fixed portion of any fragment
		
		int fieldOff =  (0==fieldCursor) ? from.templateOffset+1 : from.fragDataSize[fieldCursor];
		assert(fieldOff>=0);
		assert(fieldOff < (1<<RW_FIELD_OFF_BITS)) : "Fixed portion of a fragment can not be larger than "+(1<<RW_FIELD_OFF_BITS)+" bytes";
		final int loc = stackOff | shiftedFieldType | fieldOff;
		//         6bits       5bits       21bit 
		// high bit is going to be zero for stacks less than 32
		// low 21 is always going to be a small number offset from front of fragment.
		assert(FieldReferenceOffsetManager.extractTypeFromLoc(loc) == (shiftedFieldType>>RW_FIELD_OFF_BITS)) : "type encode decode round trip for LOC does not pass";		
		return loc;
	}

    public static int lookupToken(String target, int fragmentStart, FieldReferenceOffsetManager from) {
    	return from.tokens[lookupFragmentLocator(target,fragmentStart,from)];
    }
    
    public static int lookupSequenceLengthLoc(String target, int fragmentStart, FieldReferenceOffsetManager from) {
    	int x = lookupFragmentLocator(target, fragmentStart, from);
    	return buildFieldLoc(from, fragmentStart, x-1);
    }
	
	
    public static int lookupFragmentLocator(String target, int fragmentStart, FieldReferenceOffsetManager from) {
		int x = fragmentStart;
        		
        while (x < from.fieldNameScript.length) {
            if (target.equalsIgnoreCase(from.fieldNameScript[x])) {
            	return x;
            }
            
            boolean isGroupClosed = exitSearch(from, x);
           
            if (isGroupClosed) {
            	break;
            }
            
            x++;
        }
        throw new UnsupportedOperationException("Unable to find fragment name: "+target+" in "+Arrays.toString(from.fieldNameScript));
	}
    
    public static int lookupFragmentLocator(final long id, int fragmentStart, FieldReferenceOffsetManager from) {
		int x = fragmentStart;
        		
        while (x < from.fieldNameScript.length) {
            if (id == from.fieldIdScript[x]) {
            	return x;
            }
            
            boolean isGroupClosed = exitSearch(from, x);
           
            if (isGroupClosed) {
            	break;
            }
            
            x++;
        }
        throw new UnsupportedOperationException("Unable to find fragment id: "+id+" in "+Arrays.toString(from.fieldNameScript));
	}
    
    
    /**
     * Helpful debugging method that writes the script in a human readable form out to the console.
     * 
     * @param title
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
		
	public static boolean isTemplateStart(FieldReferenceOffsetManager from, int cursorPosition) {
		//checks the shortcut hasSimpleMessagesOnly first before any complex logic
	    return from.hasSimpleMessagesOnly || (cursorPosition<=0) || cursorPosition>=from.fragDepth.length || (from.fragDepth[cursorPosition]<=0);
	}

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + absentInt;
        result = prime * result + (int) (absentLong ^ (absentLong >>> 32));
        
        result = prime * result + MurmurHash.hash32(fieldIdScript, 0, fieldIdScript.length, 314-579-0066); //Need help call OCI
        result = prime * result + MurmurHash.hash32(tokens,        0, tokens.length,        314-579-0066); //Need help call OCI     
        result = prime * result + MurmurHash.hash32(longDefaults,  0, longDefaults.length,  314-579-0066); //Need help call OCI
        result = prime * result + MurmurHash.hash32(intDefaults,   0, intDefaults.length,   314-579-0066); //Need help call OCI
        
        int i;
        
        i = dictionaryNameScript.length;
        while (--i>=0) {
            result = prime * result + MurmurHash.hash32(dictionaryNameScript[i],  314-579-0066); //Need help call OCI
        }
        
        i = fieldNameScript.length;
        while (--i>=0) {
            result = prime * result + MurmurHash.hash32(fieldNameScript[i],  314-579-0066); //Need help call OCI
        }
        
        result = prime * result + MurmurHash.hash32(name,  314-579-0066); //Need help call OCI
        result = prime * result + preambleOffset;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        FieldReferenceOffsetManager other = (FieldReferenceOffsetManager) obj;
        if (absentInt != other.absentInt) {
            return false;
        }
        if (absentLong != other.absentLong) {
            return false;
        }
        if (!Arrays.equals(dictionaryNameScript, other.dictionaryNameScript)) {
            return false;
        }
        if (!Arrays.equals(fieldIdScript, other.fieldIdScript)) {
            return false;
        }
        if (!Arrays.equals(fieldNameScript, other.fieldNameScript)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        } else if (!name.equals(other.name)) {
            return false;
        }
        if (preambleOffset != other.preambleOffset) {
            return false;
        }
        if (!Arrays.equals(tokens, other.tokens)) {
            return false;
        }

        if (!Arrays.equals(longDefaults, other.longDefaults)) {
            return false;
        }
        if (!Arrays.equals(intDefaults, other.intDefaults)) {
            return false;
        }
          
        return true;
    }

    public long[] newLongDefaultsDictionary() {
        return RLESparseArray.rlDecodeSparseArray(longDefaults);
    }
    
    public int[] newIntDefaultsDictionary() {
        return RLESparseArray.rlDecodeSparseArray(intDefaults);
    }
    
    public void appendLongDefaults(Appendable target) throws IOException {
            Appendables.appendArray(target.append("new long[]"), '{', longDefaults, '}');
    }
    
    public void appendIntDefaults(Appendable target) throws IOException  {
            Appendables.appendArray(target.append("new int[]"), '{', intDefaults, '}');
    }
    
    public Appendable appendGUID(Appendable target) throws IOException  {
        return Appendables.appendArray(target.append("new int[]"), '{', guid, '}');
    }
    
    public Appendable appendConstuctionSource(Appendable target) throws IOException  {
        buildFROMConstructionSource(target, this, "FROM", name);
        return target;
    }
    
    
    public static FieldReferenceOffsetManager buildSingleNumberBlockFrom(
			final int fieldCount, 
			final int typeMask,
			final String name) {
		int fields = 1;
		int size = TypeMask.ringBufferFieldSize[typeMask];
		if (typeMask==TypeMask.Decimal) {
			size = 3;
			fields = 2;
		}		
		
		
		int matLen = (fields*fieldCount)+1+1;
		int[]    matrixTokens=new int[matLen];
		String[] matrixNames=new String[matLen];
		long[]   matrixIds=new long[matLen];
		matrixIds[0] = 10000;
		matrixNames[0] = name;
		
		int dataSize = (size*fieldCount)+1;
		
		if (dataSize>TokenBuilder.MAX_INSTANCE) {
			logger.info("Data size {} is too large.  Element size is {}, total count of values {} ",dataSize,size,fieldCount);
		}
		
		matrixTokens[0] = TokenBuilder.buildToken(TypeMask.Group, 0, dataSize); 
		if (typeMask==TypeMask.Decimal) {
			int m = 1;
			for (int i=1;i<=fieldCount;i++) {
				matrixIds[m] = i;
				matrixNames[m] = Integer.toString(i);
				matrixTokens[m] = TokenBuilder.buildToken(TypeMask.Decimal, 0, i); 
				m++;
				matrixIds[m] = i;
				matrixNames[m] = Integer.toString(i);
				matrixTokens[m] = TokenBuilder.buildToken(TypeMask.LongSigned, 0, i);
				m++;
			}
		} else {
			for (int i=1;i<=fieldCount;i++) {
				matrixIds[i] = i;
				matrixNames[i] = Integer.toString(i);
				matrixTokens[i] = TokenBuilder.buildToken(typeMask, 0, i);
				
			}
		}
		matrixTokens[matrixTokens.length-1] = TokenBuilder.buildToken(TypeMask.Group, OperatorMask.Group_Bit_Close, dataSize);
		//last position is left as null and zero
		assert(matrixIds[matrixIds.length-1]==0);
		assert(matrixNames[matrixNames.length-1]==null);
		FieldReferenceOffsetManager matFrom = new FieldReferenceOffsetManager(matrixTokens, /*pramble*/ (short)0, matrixNames, matrixIds);
		return matFrom;
	}

	public static <A extends Appendable>void buildFROMInterfaces(A target, String schemaName, FieldReferenceOffsetManager from) throws IOException {
        
      for(int j = 0; j<from.messageStarts.length; j++)  {
          buildFROMInterfacesSingleMessage(from.messageStarts[j], target, schemaName, from);
      } 
    }
    
    private static void buildFROMInterfacesSingleMessage(int msgIdx, Appendable target, String schemaName, 
                                                        FieldReferenceOffsetManager from) {
        
        int fragmentSize = from.fragScriptSize[msgIdx];
        String name =from.fieldNameScript[msgIdx];
        
        try {
            target.append("public interface ").append(schemaName).append(name).append("Consumer {\n");
        
            target.append("    public void consume(");
            
            boolean needComma = false;
            for(int i = 0; i<fragmentSize;i++) {
               
                
                int token = from.tokens[msgIdx+i];
            
                int type = TokenBuilder.extractType(token);
                if (type!=TypeMask.Group) {                
                    if (type==TypeMask.GroupLength) {
                        //break
                        //ends this fragment //TODO: needs recursive decent to build these.
                        //begins another nested fragment
                        //TODO: if the message has multiple fragments we must continue to build interfaces for this other types
                        throw new UnsupportedOperationException();
                    }
                                        
                    String typeName = TypeMask.primitiveTypes[type];
                    String paraName = from.fieldNameScript[msgIdx+1];
                                     
                    if (needComma) {
                        target.append(",");
                    }
                    target.append(typeName).append(' ').append(paraName);
                    needComma = true;
                }
            }
            
            target.append(");\n");
            target.append("}\n");
        
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }

    public static void buildFROMConstructionSource(Appendable target, FieldReferenceOffsetManager from, String varName, String fromName) throws IOException {
        //write out the expected source.
        target.append("\npublic final static FieldReferenceOffsetManager ");
        target.append(varName).append(" = new ").append(FieldReferenceOffsetManager.class.getSimpleName()).append("(\n");
    
        target.append("    new int[]{");
        
        boolean isFirst = true;
        for(int token:from.tokens) {
            if (!isFirst) {
                target.append(',');
            }
            target.append("0x").append(Integer.toHexString(token));
            isFirst = false;
        }
        target.append("},\n    ");
        target.append("(short)").append('0').append(",\n");// expectedFrom.preambleBytes;//TODO: swap in
    
    
        target.append("    new String[]{");
        isFirst = true;
        int runCount = 0;
        for(String tmp:from.fieldNameScript) {
            if (!isFirst) {
            	if (runCount<80) {
            		target.append(',');
            		runCount++;
            	} else {
            		target.append(",\n    ");
            		runCount=0;
            	}
            } 
            if (null==tmp) {
                target.append("null");
                runCount += 4;
            } else {
                target.append('"').append(tmp).append('\"');
                runCount += (tmp.length()+2);
            }
            isFirst = false;
        }
        target.append("},\n");
    
        try {
            Appendables.appendArray( target.append("    new long[]"), '{', from.fieldIdScript, '}').append(",\n");
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    
        target.append("    new String[]{");
        isFirst = true;
        runCount = 0;
        for(String tmp:from.dictionaryNameScript) {
            if (!isFirst) {
            	if (runCount<80) {
            		target.append(',');
            		runCount += 1;
            	} else {
            		target.append(",\n    ");
            		runCount=0;
            	}
            } 
            if (null==tmp) {
                target.append("null"); 
                runCount += 4;
            } else {
                target.append('"').append(tmp).append('\"');
                runCount += (tmp.length()+2);
            }
            isFirst = false;
        }
        target.append("},\n");
        target.append("    \"").append(fromName).append("\",\n");
        target.append("    ");
        try {
            from.appendLongDefaults(target);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        target.append(",\n");
        target.append("    ");
        try {
            from.appendIntDefaults(target);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    
        target.append(");\n\n");
    
    }

    public static String buildMsgConstName(FieldReferenceOffsetManager encodedFrom, int expectedMsgIdx) {
        return "MSG_"+encodedFrom.fieldNameScript[expectedMsgIdx].toUpperCase().replace('/', '_').replace(' ','_')+"_"+encodedFrom.fieldIdScript[expectedMsgIdx];
    }
    
    public static String buildName(FieldReferenceOffsetManager encodedFrom, int expectedMsgIdx) {
        return encodedFrom.fieldNameScript[expectedMsgIdx].replace('/', '_').replace(' ','_');
    }

	public static boolean isValidMsgIdx(FieldReferenceOffsetManager from, int msgIdx) {
		int x = from.messageStarts.length;
		while (--x>=0) {
			if (from.messageStarts[x] == msgIdx) {
				return true;//this is the start of a fragment;
			}
		}
		return false;
	}
     
}
