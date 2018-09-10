package com.ociweb.pronghorn.network.config;

public enum HTTPVerbDefaults implements HTTPVerb{

    GET("GET"),   //NoSideEffects
    HEAD("HEAD"), //NoSideEffects
    POST("POST"),
    PUT("PUT"),   //idempotent
    DELETE("DELETE"),//idempotent
    TRACE("TRACE"),//NoSideEffects //may be insecure, do not implement
    OPTIONS("OPTIONS"),//NoSideEffects
    CONNECT("CONNECT"),    
    PATCH("PATCH");   
    
    private final String key;
    private final byte[] keyBytes;
    
    private HTTPVerbDefaults(String headerKey) {
        key = headerKey;
        keyBytes = headerKey.getBytes();
    }
    
    public String getKey() {
        return key;
    }
    
    public byte[] getKeyBytes() {
    	return keyBytes;
    }
}
