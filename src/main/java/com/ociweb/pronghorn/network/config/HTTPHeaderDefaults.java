package com.ociweb.pronghorn.network.config;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.BlobReader;
import com.ociweb.pronghorn.util.Appendables;

public enum HTTPHeaderDefaults implements HTTPHeader {
    /////// 
    //NOTE: tail of both \r\n and \n are both used when these are pattern matched, do not add tail here
    ///////
	HOST("Host: %b"), 
    UPGRADE("Upgrade: %b"),
    CONNECTION("Connection: %b"),
    USER_AGENT("User-Agent: %b"),//chromium
    TRANSFER_ENCODING("Transfer-Encoding: chunked") {    
	    public <A extends Appendable> A writeValue(A target, BlobReader reader) {
	    	return target;
	    }
	}, //Transfer-Encoding: chunked
    CONTENT_LENGTH("Content-Length: %u") {    
	    public <A extends Appendable> A writeValue(A target, BlobReader reader) {
	    	Appendables.appendValue(target, reader.readPackedLong());
	    	return target;
	    }
	}, //note this captures an integer not a string
    CONTENT_TYPE("Content-Type: %b"),
    CONTENT_LOCATION("Content-Location: %b"),
    LOCATION("Location: %b"),
    ACCEPT("Accept: %b"),//chromium
    ACCEPT_CHARSET("Accept-Charset: %b"),
    ALT_SVC("Alt-Svc: %b"),//google.com
    VARY("Vary: %b"),//google.com
    ACCEPT_LANGUAGE("Accept-Language: %b"),//chromium
    ACCEPT_ENCODING("Accept-Encoding: %b"),//chromium
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
    IF_NONE_MATCH("If-None-Match: %b"), //chromium
    IF_MODIFIED_SINCE("If-Modified-Since: %b"),//chromium
    IF_RANGE("If-Range: %b"),
    IF_UNMODIFIED_SINCE("If-Unmodified-Since: %b"),
    VIA("Via: %b"),
    WARNING("Warning: %b"),
    DNT("DNT: %b"),//chromium
    SEC_WEBSOCKET_KEY("Sec-WebSocket-Key: %b"),
    SEC_WEBSOCKET_PROTOCOL("Sec-WebSocket-Protocol: %b"),
    SEC_WEBSOCKET_VERSION("Sec-WebSocket-Version: %b"),
    ORIGIN("Origin: %b"),
    PRAGMA("Pragma: %b"), //Not matching?
    SERVER("Server: %b"), //Not matching?
    STATUS("Status: %i %b"){    
	    public <A extends Appendable> A writeValue(A target, BlobReader reader) {
	    	try {
	    		Appendables.appendValue(target, reader.readPackedLong());				
	    		target.append(' ');
	    		reader.readUTF(target);
			} catch (IOException e) {
				e.printStackTrace();
			}
	    	return target;
	    }
	}, //Not matching?
    KEEP_ALIVE("Keep-Alive: %b"),
    EXPIRES("Expires: %b"),
    RETRY_AFTER("Retry-After: %b"), //CNN
    X_SERVED_BY("X-Served-By: %b"), //CNN
    X_CACHE("X-Cache: %b"), //CNN
    X_CACHE_HITS("X-Cache-Hits: %b"), //CNN
    
//    CONTENT_SECURITY_POLICY("content-security-policy: %b"), //twitter
    SET_COOKIE("set-cookie: %b"), //twitter
    COOKIE("Cookie: %b"), //chromium
    STRICT_TRANSPORT_SECURITY("strict-transport-security: %b"), //twitter
    X_CONNECTION_HASH("x-connection-hash: %b"), //twitter
    X_RESPONSE_TIME("x-response-time: %b"), //twitter
    X_XSS_PROTECTION("x-xss-protection: %b"), //twitter
    X_CONTENT_TYPE_OPTIONS("x-content-type-options: %b"), //twitter
    X_FRAME_OPTIONS("x-frame-options: %b"), //twitter
    X_TRANSACTION("x-transaction: %b"), //twitter
 //   CONTENT_DISPOSITION("content-disposition:  %b"), //twitter
    X_TSA_REQUEST_BODY_TIME("x-tsa-request-body-time: %b"), //twitter
    TSA("tsa: %b"), //twitter    
    X_TWITTER_RESPONSE_TAGS("x-twitter-response-tags: %b"), //twitter
    X_UA_COMPATIBLE("x-ua-compatible: %b"), //twitter
    WWW_AUTHENTICATE("www-authenticate: %b"), //twitter    
    ML("ml: %b"), //twitter
    UPGRADE_INSECURE_REQUESTS("Upgrade-Insecure-Requests: %u"), //chromium
    P3P("P3P: %b"),
    X_FORWARD_FOR("x-Forwarded-For: %b"),
    X_FORWARD_HOST("x-Forwarded-Host: %b"),
    X_ONLINE_HOST("x-Online-Host: %b"),
    X_FRONT_END_HTTPS("Front-End-Https: %b"),
    X_ATT_DEVICEID("x-ATT-DeviceId: %b"),
    X_WAP_PROFILE("x-Wap-Profile: %b");
            
    private CharSequence readingTemplate; //used for reading headers, must be lower case to do all case insinsitve matching
    private CharSequence writingRoot; //used for writing headers.
    private byte[] rootBytes;
    
    private HTTPHeaderDefaults(String template) {
        this.readingTemplate = template.toLowerCase();
        this.writingRoot = template;
        int i = 0;
        int lim = template.length()-1;
        while (i<lim) {

        	if (template.charAt(i)==':' && template.charAt(i+1)==' ') {
        		writingRoot = template.subSequence(0, i+2);
        		rootBytes = readingTemplate.subSequence(0, i+2).toString().getBytes();
        		break;
        	}
        	i++;
        }
    }

    public CharSequence readingTemplate() {
        return readingTemplate;
    }

    public CharSequence writingRoot() {
        return writingRoot;
    }
    
    public <A extends Appendable> A writeValue(A target, BlobReader reader) {
    	reader.readUTF(target);
    	return target;
    }
    
    
    public byte[] rootBytes() {
    	return rootBytes;
    }
    
}
