package com.ociweb.jfast.ring;

import java.util.Arrays;

public abstract class FieldReferenceOffsetManager {
	
	public static final int SEQ     = 0x10000000;
	public static final int MSG_END = 0x80000000;
	
    public int preambleOffset; //-1 if there is no preamble
    public int templateOffset;
    
    public int tokensLen;
    public int[] fragDataSize;
    public int[] fragScriptSize;
    public int[] tokens;
    public int[] starts;
    public int[] limits;
    public String[] fieldNameScript;
    public int maximumFragmentStackDepth;
    
  //TODO: convert to static
    public final int fieldCount(int templateId) {
    	return 1+ limits[templateId]-starts[templateId];
    }
    
  //TODO: convert to static
    public final String fieldName(int templateId, int position) {
    	return fieldNameScript[starts[templateId]+position];
    }
    
    
    //TODO: convert to static
    public final int lookupIDX(int templateId, String target) {
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
