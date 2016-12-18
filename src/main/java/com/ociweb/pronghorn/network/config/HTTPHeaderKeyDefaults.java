package com.ociweb.pronghorn.network.config;

public enum HTTPHeaderKeyDefaults implements HTTPHeaderKey {
   
    //NOTE: tail of both \r\n and \n are both used when these are pattern matched, do not add tail here
    HOST("Host: %b"), 
    UPGRADE("Upgrade: %b"),
    CONNECTION("Connection: %b"),
    USER_AGENT("User-Agent: %b"),
    TRANSFER_ENCODING("Transfer-Encoding: chunked"), //Transfer-Encoding: chunked
    CONTENT_LENGTH("Content-Length: %u"), //note this captures an integer not a string
    CONTENT_TYPE("Content-Type: %b"),
    ACCEPT("Accept: %b"),
    ACCEPT_CHARSET("Accept-Charset: %b"),
    ACCEPT_LANGUAGE("Accept-Language: %b"),
    ACCEPT_ENCODING("Accept-Encoding: %b"),
    ACCEPT_DATETIME("Accept-Datetime: %b"),
    AUTHORIZATION("Authorization: %b"),
    CACHE_CONTROL("Cache-Control: %b"),
    EXPECT("Expect: %b"),
    FORWARDED("Forwarded: %b"),
    FROM("From: %b"),
    IF_MODIFIED_SINCE("If-Modified-Since: %b"),
    IF_RANGE("If-Range: %b"),
    IF_UNMODIFIED_SINCE("If-Unmodified-Since: %b"),
    VIA("Via: %b"),
    WARNING("Warning: %b"),
    DNT("DNT: %b"),
    SEC_WEBSOCKET_KEY("Sec-WebSocket-Key: %b"),
    SEC_WEBSOCKET_PROTOCOL("Sec-WebSocket-Protocol: %b"),
    SEC_WEBSOCKET_VERSION("Sec-WebSocket-Version: %b"),
    ORIGIN("Origin: %b"),
    PRAGMA("Pragma: %b"),
    SERVER("Server: %b"),
    EXPIRES("Expires: %b"),
    X_FORWARD_FOR("X-Forwarded-For: %b"),
    X_FORWARD_HOST("X-Forwarded-Host: %b"),
    X_ONLINE_HOST("X-Online-Host: %b"),
    X_FRONT_END_HTTPS("Front-End-Https: %b"),
    X_ATT_DEVICEID("X-ATT-DeviceId: %b"),
    X_WAP_PROFILE("X-Wap-Profile: %b");
            
    private CharSequence key;
    
    private HTTPHeaderKeyDefaults(CharSequence headerKey) {
        key = headerKey;
    }
    
    public CharSequence getKey() {
        return key;
    }

}
