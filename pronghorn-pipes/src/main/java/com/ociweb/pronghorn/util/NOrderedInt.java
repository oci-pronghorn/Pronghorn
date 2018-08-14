package com.ociweb.pronghorn.util;

import java.io.IOException;

import com.ociweb.pronghorn.util.Appendables;

public class NOrderedInt {

    private final int[] elements;
    private final long[] occurences;
    private int count;
    
    public NOrderedInt(int maxCount) {
        
        elements = new int[maxCount];        
        occurences = new long[maxCount];
        
    }
    
    public void sample(int item) {
        
        for(int i = 0; i<count; i++) {
            if (item==(elements[i])) {
                occurences[i]++;
                int j=i;
                while ((j > 0) && (occurences[j-1] < occurences[i])) {
                    j--;
                }
                
                if (j!=i) {
                    long tOcc = occurences[i];
                    int tEle = elements[i];
                 
                    System.arraycopy(elements, j, elements, j+1, i-j);
                    System.arraycopy(occurences, j, occurences, j+1, i-j);
                    
                    occurences[j] = tOcc;
                    elements[j]   = tEle;
                }   
                return;
            }            
        }     
        //if we are here just add this one on to the end
        if (count<occurences.length) {
            elements[count] = item;
            occurences[count++]=0;
        }        
        
        
    }
    
    public void writeTo(Appendable target) {
    
        try {
  
            for(int i = 0; i<count; i++) {
                Appendables.appendValue(Appendables.appendValue(target, elements[i]).append(":"),occurences[i]).append(", ");
            }
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        
    }
    
}
