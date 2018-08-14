package com.ociweb.pronghorn.util;

import java.io.IOException;

import com.ociweb.pronghorn.util.Appendables;

public class NOrdered<T> {

    private final Object[] elements;
    private final long[] occurences;
    private int count;
    
    public NOrdered(int maxCount) {
        
        elements = new Object[maxCount];        
        occurences = new long[maxCount];
        
    }
    
    public void sample(T item) {
        
        for(int i = 0; i<count; i++) {
            if (item.equals(elements[i])) {
                occurences[i]++;
                int j=i;
                while ((j > 0) && (occurences[j-1] < occurences[i])) {
                    j--;
                }
                
                if (j!=i) {
                    long tOcc = occurences[i];
                    Object tEle = elements[i];
                 
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
                Appendables.appendValue(target.append(elements[i].toString()).append(":"),occurences[i]).append(", ");
            }
            
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        
        
    }
    
}
