package com.ociweb.jfast.catalog.loader;

import java.util.Arrays;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.generator.GeneratorUtils;

public class FieldReferenceOffsetManager {

	public static final FieldReferenceOffsetManager TEST = new FieldReferenceOffsetManager();
	
    public final int preambleOffset; //-1 if there is no preamble
    public final int templateOffset;
    
    public static final int SEQ     = 0x10000000;
    public static final int MSG_END = 0x80000000;
    public final int tokensLen;
    public final int[] fragDataSize;
    public final int[] fragScriptSize;
    public final int[] tokens;
    public final int[] starts;
    public final int[] limits;
    public final String[] fieldNameScript;
    public final int maximumFragmentStackDepth;
    
    /**
     * Constructor is only for unit tests.
     */
    private FieldReferenceOffsetManager() {
    	
        //TODO: B, clientConfig must be able to skip reading the preamble,
        int PREAMBLE_MASK = 0xFFFFFFFF;//Set to zero when we are not sending the preamble
        
        //contants for basic test setups.
        int configPreambleBytes = 0;
        
        int pb = PREAMBLE_MASK & configPreambleBytes;
        if (pb<=0) {
            preambleOffset = -1;
            templateOffset = 0;
        } else {
            preambleOffset = 0;
            templateOffset = (pb+3)>>2;
        }
         
        fragDataSize = null;
        fragScriptSize = null;
        maximumFragmentStackDepth = 10; //default for testing

        tokens = null;
        tokensLen = null==tokens?0:tokens.length;
        
        starts = null;
        limits = null;
        
        fieldNameScript = null;
        
    }
    
    public FieldReferenceOffsetManager(TemplateCatalogConfig config) {
        
        //TODO: B, clientConfig must be able to skip reading the preamble,
        int PREAMBLE_MASK = 0xFFFFFFFF;//Set to zero when we are not sending the preamble
        
        int pb = PREAMBLE_MASK & config.clientConfig.getPreableBytes();
        if (pb<=0) {
            preambleOffset = -1;
            templateOffset = 0;
        } else {
            preambleOffset = 0;
            templateOffset = (pb+3)>>2;
        }
         
        if (null==config || 
            null == config.scriptTokens) {
            fragDataSize = null;
            fragScriptSize = null;
            maximumFragmentStackDepth = 0;
        } else {
        
            fragDataSize  = new int[config.scriptTokens.length]; //size of fragments and offsets to fields, first field of each fragment need not use this!
            fragScriptSize = new int[config.scriptTokens.length];
            //TODO: D, could be optimized after the fragments are given the expected locations, for now this works fine.
            maximumFragmentStackDepth = config.scriptTokens.length;
            
            buildFragScript(config);
        }
        tokens = config.scriptTokens;
        tokensLen = null==tokens?0:tokens.length;
        
        starts = config.getTemplateStartIdx();
        limits = config.getTemplateLimitIdx();
        
        fieldNameScript = config.fieldNameScript();
        
        
    }

    //TODO: C, move this into TemplateCatalog save?
    private void buildFragScript(TemplateCatalogConfig config) {
        int[] scriptTokens = config.scriptTokens;
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
            int type = TokenBuilder.extractType(config.scriptTokens[i]);
            boolean isGroup = TypeMask.Group == type;    
            boolean isGroupOpen = isGroup && (0 == (config.scriptTokens[i] & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isGroupClosed = isGroup && (0 != (config.scriptTokens[i] & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isSeqLength = TypeMask.GroupLength == type;
                      
            if (isGroupOpen || nextTokenOpensFragment) {
                if (debug) {
                    System.err.println();
                }
                depth++;                
                fragmentStartIdx = i;       
                
                boolean isSeq = (0 != (config.scriptTokens[i] & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER)));
                //TODO: if optional group it will also need to be zero like seq
                
                //must be a group open only for a new message 
                if (!isSeq && isGroupOpen) { 
                    int preambleInts = (config.clientConfig().getPreableBytes()+3)>>2;
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
            
            int token = config.scriptTokens[i];
            
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
    
    public int fieldCount(int templateId) {
    	return 1+ limits[templateId]-starts[templateId];
    }
    
    public String fieldName(int templateId, int position) {
    	return fieldNameScript[starts[templateId]+position];
    }
    
    

    public int lookupIDX(int templateId, String target) {
        int x = starts[templateId];
        int limit = limits[templateId];
        
        
        int UPPER_BITS = 0xF0000000;
        //System.err.println("looking for "+target+ " between "+x+" and "+limit);
        //System.err.println(Arrays.toString(fieldNameScript));
        
        while (x<=limit) {
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
            x++;
            
        }
        throw new UnsupportedOperationException("Unable to find field name: "+target+" in "+Arrays.toString(fieldNameScript));
        
    }

}
