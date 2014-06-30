package com.ociweb.jfast.loader;

import java.util.Arrays;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;

public class FieldReferenceOffsetManager {

    public final int preambleOffset; //-1 if there is no preamble
    public final int templateOffset;
    
    public static final int SEQ     = 0x10000000;
    public static final int MSG_END = 0x80000000;
    
    public final int[] fragDataSize;
    public final int[] fragScriptSize;
    public final int[] tokens;
    public final int[] starts;
    
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
        } else {
        
            fragDataSize  = new int[config.scriptTokens.length]; //size of fragments and offsets to fields, first field of each fragment need not use this!
            fragScriptSize = new int[config.scriptTokens.length];
            
            buildFragScript(config);
        }
        tokens = config.scriptTokens;
        starts = config.getTemplateStartIdx();
        
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
                    fragDataSize[fragmentStartIdx] = 2; //TODO: where does this 2 come from it needs to know about the preamble
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
            fragDataSize[fragmentStartIdx]+=TypeMask.ringBufferFieldSize[TokenBuilder.extractType(token)];
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

}
