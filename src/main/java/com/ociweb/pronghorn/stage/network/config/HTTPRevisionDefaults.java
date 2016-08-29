package com.ociweb.pronghorn.stage.network.config;

public enum HTTPRevisionDefaults implements HTTPRevision {

    HTTP_0_9("HTTP/0.9"), 
    HTTP_1_0("HTTP/1.0"),
    HTTP_1_1("HTTP/1.1");
    
    private CharSequence key;
    private byte[] bytes;
    
    private HTTPRevisionDefaults(String headerKey) {
        key = headerKey;
        bytes = headerKey.trim().getBytes();
    }
    
    public CharSequence getKey() {
        return key;
    }
    
    public byte[] getBytes() {
        return bytes;
    }

}
