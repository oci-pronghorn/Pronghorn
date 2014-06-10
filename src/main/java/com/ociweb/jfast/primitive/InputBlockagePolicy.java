package com.ociweb.jfast.primitive;

public interface InputBlockagePolicy {  

    public void detectedInputBlockage(int need, FASTInput input);
    
    public void resolvedInputBlockage(FASTInput input);
    
}