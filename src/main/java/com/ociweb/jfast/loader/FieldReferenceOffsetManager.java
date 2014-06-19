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
    public final int[] fragJumps;
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
         
        if (null==config || null == config.scriptTokens) {
            fragDataSize = null;
            fragJumps = null;
            fragScriptSize = null;
        } else {
        
            fragDataSize  = new int[config.scriptTokens.length]; //size of fragments and offsets to fields, first field of each fragment need not use this!
            fragJumps = new int[config.scriptTokens.length];
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
        int[] tokenStops = config.templateScriptEntryLimits;
        int tokenStopIdx = 0;
        boolean debug = true;       
        int i = 0;      
        int fragmentStartIdx=0;
        
        //when a group or sequence is absent in the buffer we need to jump over it and any internal structures in the script
        //all sequence counters must be incremented when an internal one increments
        int[] scriptFragStartStack = new int[scriptLength];//TODO: B, this is now producing garbage! Temp space must be held by temp space owner!
        
        
        int depth = 0; //need script jump number
        
        //TODO: how do we know when in the script we have reached a new template/message?  Must set high bit there?
        
        while (i<scriptLength) {            
            //now past the end of the template so 
            //close it because this index starts a new one
            //first position is always part of a new template
            
            //sequences and optional groups will always have group tags.
            boolean isGroup = TypeMask.Group == TokenBuilder.extractType(config.scriptTokens[i]);    
            boolean isGroupOpen = isGroup && (0 == (config.scriptTokens[i] & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            boolean isGroupClosed = isGroup && (0 != (config.scriptTokens[i] & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            
            boolean isStop = i==tokenStops[tokenStopIdx];
            
                        
            
            if (isGroupClosed) {
                depth--;
            }
            
            //detected new token
            if (i==0 ||               //first index must always be a fragment open
         //       (isStop && !isGroupClosed) ||
                isGroupOpen) {        //open groups(seq/grp/msg) cause previous fragment to close and new one to open

                if (isStop) {                    
                    depth--;
                    
                    tokenStopIdx++;
                    //this is not an inner group but is really the templates pmap 
                    if (TypeMask.Group == TokenBuilder.extractType(config.scriptTokens[i])) {
                        //if is open then we must inc i            
                    } 
                    scriptFragStartStack[depth]=i;
                    depth++;
                    //end of message is here OR at end of script
                    fragJumps[i] |= MSG_END;
                }
                fragmentStartIdx = i;     
                fragDataSize[fragmentStartIdx] =2;
                
                if (isGroupOpen) {
                    boolean isSeq = (0 != (config.scriptTokens[i] & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER)));
                    fragJumps[i] |= isSeq ? SEQ : 0; //type base  
                    if (isSeq) {
                        fragDataSize[fragmentStartIdx] = 0;
                    }
                    scriptFragStartStack[depth]=i;
                    depth++;
                }      
                
                if (debug) {
                    System.err.println(depth);
                }
                
            }
            int token = config.scriptTokens[i];
            
            fragDataSize[i]=fragDataSize[fragmentStartIdx]; //keep the individual offsets per field
            fragDataSize[fragmentStartIdx]+=TypeMask.ringBufferFieldSize[TokenBuilder.extractType(token)];
            fragScriptSize[fragmentStartIdx]++;
            
            //TODO: using the above approch we need to total the length of the frag
            
            
            int j = depth;
            while (--j>=0) {//TODO: rename fragJumps as group jumps, this is already part of the group jump numbers!!!
                fragJumps[scriptFragStartStack[j]]++; //TODO: these jumps are only needed for ? jumping over SEQ/GRP and all children when skipped?
            }
           // fragJumps[templateIdx]++;//jump for script position changes
            if (debug) {
                System.err.println(depth+"  "+i+"  "+TokenBuilder.tokenToString(scriptTokens[i]));
            }
            
            i++;
        }
                
        if (debug) {
            System.err.println(Arrays.toString(fragDataSize));
            System.err.println(Arrays.toString(fragScriptSize));
            
            System.err.println(Arrays.toString(fragJumps));
        }
    }

}
