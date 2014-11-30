package com.ociweb.jfast.generator;

import java.io.IOException;
import java.util.Arrays;

public class BalancedSwitchGenerator {
    
    final String varName;
    final String tabSize = "    ";
    final int bits = 32;
    int[] counts = new int[bits];
    final boolean validate = false;
    
    public BalancedSwitchGenerator(String varName) {
    	this.varName = varName;
    }
    
    public Appendable generate(String tab, Appendable target, int[] values, String[] code) {
        if (values.length>0) {
            try {            	
                split(tab,target,values, code);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } 
        return target;
    }
    
    private void split(String tab, Appendable target, int[] values, String[] code) throws IOException {

        if (values.length==1) {
            if (validate) {
            	target.append(tab).append("assert(").append(Integer.toString(values[0])).append("==").append(varName).append(") : \"found value of \"+").append(varName).append(";\n");
            }
            target.append(tab).append(code[0].replace("\n\r", "\n"+tab));
            return;
        }

        
        int splitBit = computeSplitBit(values);
        
        int countOfOnes = totalOnesSide(values, splitBit);
        int[][] splitValues = new int[][]{new int[values.length - countOfOnes],new int[countOfOnes]};
        String[][] splitCode = new String[][]{new String[values.length - countOfOnes],new String[countOfOnes]};
        
        int[] x = new int[]{0,0};
        int i = values.length;
        while(--i>=0) {
            int idx = (1&(values[i]>>>splitBit));
            splitValues[idx][x[idx]] = values[i];
            splitCode[idx][x[idx]++] = code[i];
        }
        assert(splitValues[0].length>0) : "Split bit "+splitBit+"  "+Arrays.toString(splitValues[1]);
        assert(splitValues[1].length>0) : "Split bit "+splitBit+"  "+Arrays.toString(splitValues[0]);
        
        //TODO: D, if division only pulls off one item and this is the biggest split possible then we should reconsider the strategy for the rest.
        //      go back and check with more than 1 bit in mask and continue until a good split is found.
        
        int maskBit = 1<<splitBit;

        target.append(tab).append("if ((").append(varName).append("&0x").append(Integer.toHexString(maskBit)).append(")==0").append(") {\n");
        split(tabSize+tab,target,splitValues[0],splitCode[0]);
        target.append(tab).append("} else {\n");
        split(tabSize+tab,target,splitValues[1],splitCode[1]);
        target.append(tab).append("}\n");
        
    }

    private int totalOnesSide(int[] values, int splitBit) {
        int ones = 0;
        int i = values.length;
        while(--i>=0) {
            //total up the ones.
            ones+=(1&(values[i]>>>splitBit));
        }
        return ones;
    }

    private int computeSplitBit(int[] values) {
        //clear
        int j = bits;
        while (--j>=0) {
            counts[j]=0;
        }
        
        //map totals
        int i = values.length;
        while (--i>=0) {
            j = bits;
            while (--j>=0) {
                counts[j]+=(1&(values[i]>>>j));
            }
        }
        
        //reduce to single bit
        int half = values.length>>1;
        int bit = 0;
        int dist = Math.abs(half-counts[0]);
        j = bits;
        while (--j>=0) {
            int tmp = Math.abs(half-counts[j]);
            if (tmp<=dist && //pick a bit the splits the most
                counts[j]!=0 &&  //dont pick a bit that does not split
                counts[j]!=values.length) {
                bit = j;
                dist = tmp;
            }
        }
        return bit;
    }
    
}
