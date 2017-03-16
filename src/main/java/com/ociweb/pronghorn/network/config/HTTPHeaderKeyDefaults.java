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
    CONTENT_LOCATION("Content-Location: %b"),
    ACCEPT("Accept: %b"),
    ACCEPT_CHARSET("Accept-Charset: %b"),
    ACCEPT_LANGUAGE("Accept-Language: %b"),
    ACCEPT_ENCODING("Accept-Encoding: %b"),
    ACCEPT_DATETIME("Accept-Datetime: %b"),
    AUTHORIZATION("Authorization: %b"),
    CACHE_CONTROL("Cache-Control: %b"),
    DATE("Date: %b"),//nginx
    LAST_MODIFIED("Last-Modified: %b"),//nginx
    ETAG("ETag: %b"),//nginx
    ACCEPT_RANGES("Accept-Ranges: %b"),//nginx
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
    PRAGMA("Pragma: %b"), //Not matching?
    SERVER("Server: %b"), //Not matching?
    STATUS("Status: %i %b"), //Not matching?
    
    EXPIRES("Expires: %b"),
    
//    CONTENT_SECURITY_POLICY("content-security-policy: %b"), //twitter
    SET_COOKIE("set-cookie: %b"), //twitter
    STRICT_TRANSPORT_SECURITY("strict-transport-security: %b"), //twitter
    X_CONNECTION_HASH("x-connection-hash: %b"), //twitter
    X_RESPONSE_TIME("x-response-time: %b"), //twitter
    X_XSS_PROTECTION("x-xss-protection: %b"), //twitter
    X_CONTENT_TYPE_OPTIONS("x-content-type-options: %b"), //twitter
    X_FRAME_OPTIONS("x-frame-options: %b"), //twitter
    X_TRANSACTION("x-transaction: %b"), //twitter
 //   CONTENT_DISPOSITION("content-disposition:  %b"), //twitter
    X_TSA_REQUEST_BODY_TIME("x-tsa-request-body-time: %b"), //twitter
    X_TWITTER_RESPONSE_TAGS("x-twitter-response-tags: %b"), //twitter
    X_UA_COMPATIBLE("x-ua-compatible: %b"), //twitter
    ML("ml: %b"), //twitter
    
    X_FORWARD_FOR("X-Forwarded-For: %b"),
    X_FORWARD_HOST("X-Forwarded-Host: %b"),
    X_ONLINE_HOST("X-Online-Host: %b"),
    X_FRONT_END_HTTPS("Front-End-Https: %b"),
    X_ATT_DEVICEID("X-ATT-DeviceId: %b"),
    X_WAP_PROFILE("X-Wap-Profile: %b");
            
    private CharSequence template;
    private CharSequence root;
    
    
    private HTTPHeaderKeyDefaults(CharSequence template) {
        this.template = template;
        this.root = template;
        int i = 0;
        int lim = template.length()-1;
        while (i<lim) {
        	if (template.charAt(i)==':' && template.charAt(i+1)==' ') {
        		root = template.subSequence(0, i+2);
        		break;
        	}
        	i++;
        }
    }
    
    public CharSequence getKey() {
        return template;
    }
    
    public CharSequence getRoot() {
        return root;
    }

}
