package com.ociweb.jfast.primitive;

public interface OutputBlockagePolicy {

    public void detectedInputBlockage(int need, FASTOutput output);
    
    public void resolvedInputBlockage(FASTOutput output);
    
}
