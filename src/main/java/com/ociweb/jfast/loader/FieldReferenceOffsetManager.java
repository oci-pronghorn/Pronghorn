package com.ociweb.jfast.loader;

import java.util.Arrays;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;

public class FieldReferenceOffsetManager {

    public final int preambleOffset; //-1 if there is no preamble
    public final int templateOffset;
    
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
               
        //TODO: must add the groups back in when they have no pmap?
        
        //TODO A. Do all this in a single pass.
       /* 
        //must build array of fragment sizes based on script
        //must build jump points and rules from script
        //must build the offset for each field as this is done for each point in script
        //Put offests in map under "Name" and "Id"
        
        
        //build the array of step size
        int[] script = config.scriptFieldIds;
        int[] scriptTokens = config.scriptTokens;
        
        //////////////////////////////////////////////////////////////////////////////
        //TODO: new method here to buld this.
         * loop on index
         * check template 
         * walk to end of template
         * requries stack and should be modular walker that can be used to replace other usages.
         * 
        //at fragment location give size of fragment (for field positions give the summed offset)
        //at fragment location give 2 possible next jump locations
        //at fragment location give bit for how to jump.
        ///////////////////////////////////////////////////////////////////////////////////////
        
        System.err.println(Arrays.toString(config.templateScriptEntries));
        System.err.println(Arrays.toString(config.templateScriptEntryLimits));
        
   //     config.
        
        int len = script.length;
        int[] fieldSizes    = new int[len];
        int[] fragmentSizes = new int[len];
        int[][] nextFrag    = new int[2][];
        nextFrag[0] = new int[len];
        nextFrag[1] = new int[len];
                
        int sum = 0;
        int sumIdx = 0;
        
        int i = 0;
        while (i<len) {
        
            int size = stepSizeInRingBuffer(scriptTokens[i]);
            fieldSizes[i] =  size;
            sum+=size;

            if (TypeMask.Group == TokenBuilder.extractType(scriptTokens[i])) {
                boolean isOpen = (0 == (scriptTokens[i] & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
                if (isOpen) {
                    sum = 0;
                    sumIdx = i;
                    System.err.println(i);
                } else {
                    fragmentSizes[sumIdx] = sum;   
                    nextFrag[0][sumIdx] = i+1;
//                    if (i+1<len) {
//                     //   sum = 0;
//                     //   sumIdx = i+1;
//                        System.err.println(i+1);
//                    }
                }
                
                
            } else {
                int j = config.templateScriptEntries.length;
                while (--j>=0) {
                    if (config.templateScriptEntries[j]==i) {
                        fragmentSizes[sumIdx] = sum;   
                        nextFrag[0][sumIdx] = i+1;
                        sum = 0;
                        sumIdx = i;
                    }
                }
                
                
            }
            
            
//            else if (TypeMask.GroupLength == TokenBuilder.extractType(scriptTokens[i])) {
//                sum = 0;
//                sumIdx = i;
//                System.err.println(i);
//            }
            
            
            
            
            i++;;
        }
        
        System.err.println("I:"+Arrays.toString(script));
        
        System.err.println("B:"+Arrays.toString(fieldSizes));
        
        System.err.println("C:"+Arrays.toString(fragmentSizes));
        
        System.err.println("D:"+Arrays.toString(nextFrag[0]));
        System.err.println("E:"+Arrays.toString(nextFrag[1]));
        
        */
        
        
        
    }

    //TODO: A, Where does the lookup go?
    public static int stepSizeInRingBuffer(int token) {
        //TODO: C, Convert to array lookup
        
        int stepSize = 0;
        if (0 == (token & (16 << TokenBuilder.SHIFT_TYPE))) {
            // 0????
            if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                // 00???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    // int
                    stepSize = 1;
                } else {
                    // long
                    stepSize = 2;
                }
            } else {
                // 01???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    // int for text (takes up 2 slots)
                    stepSize = 2;
                } else {
                    // 011??
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        // 0110? Decimal and DecimalOptional
                        stepSize = 3;
                    } else {
                        // int for bytes
                        stepSize = 2;
                    }
                }
            }
        } else {
            if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                // 10???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    // 100??
                    // Group Type, no others defined so no need to keep checking
                    stepSize = 0;
                } else {
                    // 101??
                    // Length Type, no others defined so no need to keep
                    // checking
                    // Only happens once before a node sequence so push it on
                    // the count stack
                    stepSize = 1;
                }
            } else {
                // 11???
                // Dictionary Type, no others defined so no need to keep
                // checking
                stepSize = 0;
            }
        }
    
        return stepSize;
    }

}
