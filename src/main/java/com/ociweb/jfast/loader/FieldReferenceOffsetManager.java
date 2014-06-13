package com.ociweb.jfast.loader;

import java.util.Arrays;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;

public class FieldReferenceOffsetManager {

    public final int preambleOffset; //-1 if there is no preamble
    public final int templateOffset;
    
    private static final int SEQ = 0x10000000;
    private static final int GRP = 0x20000000;
    
    public final int[] fragSize;
    public final int[] fragJumps;
    
    
    public FieldReferenceOffsetManager(TemplateCatalogConfig config) {
        
        //TODO: B, clientConfig must be able to skip reading the preamble,
        
        int pb = config.clientConfig.getPreableBytes();
        if (pb<=0) {
            preambleOffset = -1;
            templateOffset = 0;
        } else {
            preambleOffset = 0;
            templateOffset = (pb+3)>>2;
        }
         
        if (null==config || null == config.scriptTokens) {
            fragSize = null;
            fragJumps = null;
        } else {
        
            fragSize  = new int[config.scriptTokens.length]; //size of fragments and offsets to fields, first field of each fragment need not use this!
            fragJumps = new int[config.scriptTokens.length];
            
            buildFragScript(config);
        }
    }

    private void buildFragScript(TemplateCatalogConfig config) {
        int[] scriptTokens = config.scriptTokens;
        int scriptLength = scriptTokens.length;        
        int[] tokenStops = config.templateScriptEntryLimits;
        int tokenStopIdx = 0;
        
        int i = 0;
        
        //JUMPS:
        //the next block immediatly follows or we jump over it in the script so all we need is the jump number in addition to our own size.
        //add top bits for the 3 kinds of jumps at the top of the int.
        //in the same int save the scirpt size of this fragment, when doing jump over just jump twice.
        // use two arrays, one for the script size.
        
        boolean debug = false;

        //TODO: A, DOES NOT SUPPORT NESTED GROUPS SO WE CAN EXCLUDE CHILDREN, REQURIES STACK CHANGE.
        
        int templateIdx=0;
        
        while (i<scriptLength) {            
            //now past the end of the template so 
            //close it because this index starts a new one
            //first position is always part of a new template
            boolean isStop = i==tokenStops[tokenStopIdx];
            boolean isGroupOpen = (TypeMask.Group == TokenBuilder.extractType(config.scriptTokens[i])) &&
                                   (0 == (config.scriptTokens[i] & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
            if (i==0 || isStop || isGroupOpen) {
                if (isStop) {
                    tokenStopIdx++;
                    //this is not an inner group but is really the templates pmap 
                    if (TypeMask.Group == TokenBuilder.extractType(config.scriptTokens[i])) {
                        //if is open then we must inc i            
                    } 
                }
                if (debug) {
                    System.err.println();
                }
                templateIdx = i;     
                
                if (isGroupOpen) {
                    boolean isSeq = (0 == (config.scriptTokens[i] & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER)));
                    fragJumps[templateIdx] = isSeq ? SEQ : GRP; //type base                      
                }                
            }
            int token = config.scriptTokens[i];
            int stepSize = TypeMask.ringBufferFieldSize[TokenBuilder.extractType(token)];
            fragSize[i]=fragSize[templateIdx]; //keep the individual offsets per field
            fragSize[templateIdx]+=stepSize;   //keep the total for the template size 
            
            fragJumps[templateIdx]++;//jump for script position changes
            if (debug) {
                System.err.println(i+" "+TokenBuilder.tokenToString(scriptTokens[i]));
            }
            
            i++;
        }
                
        if (debug) {
            System.err.println(Arrays.toString(fragSize));
            System.err.println(Arrays.toString(fragJumps));
        }
    }

}
