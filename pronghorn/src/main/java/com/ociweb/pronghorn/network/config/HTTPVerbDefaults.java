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
    
    private CharSequence key;
    
    private HTTPVerbDefaults(CharSequence headerKey) {
        key = headerKey;
    }
    
    public CharSequence getKey() {
        return key;
    }
    
}
