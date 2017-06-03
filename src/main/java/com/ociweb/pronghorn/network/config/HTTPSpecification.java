package com.ociweb.pronghorn.network.config;

import com.ociweb.pronghorn.util.TrieParser;

public class HTTPSpecification  <   T extends Enum<T> & HTTPContentType,
                                    R extends Enum<R> & HTTPRevision,
                                    V extends Enum<V> & HTTPVerb,
                                    H extends Enum<H> & HTTPHeader
                                    > {
    
    public final Class<T> supportedHTTPContentTypes;
    public final Class<R> supportedHTTPRevisions;
    public final Class<V> supportedHTTPVerbs;
    public final Class<H> supportedHTTPHeaders;
    
    public final int maxVerbLength;
   
    public final int headerCount;
    public final H[] headers;
    public final T[] contentTypes;
    public final V[] verbs;
    public final R[] revisions;
    
    private boolean trustAccurateStrings = true;
    private final TrieParser headerParser;
    
    private static HTTPSpecification<HTTPContentTypeDefaults,HTTPRevisionDefaults,HTTPVerbDefaults,HTTPHeaderDefaults> defaultSpec;
    
    public static HTTPSpecification<HTTPContentTypeDefaults,HTTPRevisionDefaults,HTTPVerbDefaults,HTTPHeaderDefaults>  defaultSpec() {
        if (null == defaultSpec) {
            defaultSpec = new HTTPSpecification(HTTPContentTypeDefaults.class, HTTPRevisionDefaults.class, HTTPVerbDefaults.class,  HTTPHeaderDefaults.class );
        } 
        return defaultSpec;
    }
    
    private HTTPSpecification(Class<T> supportedHTTPContentTypes, Class<R> supportedHTTPRevisions, Class<V> supportedHTTPVerbs, Class<H> supportedHTTPHeaders) {

        this.supportedHTTPContentTypes = supportedHTTPContentTypes;
        this.supportedHTTPRevisions = supportedHTTPRevisions;
        this.supportedHTTPVerbs = supportedHTTPVerbs;
        this.supportedHTTPHeaders = supportedHTTPHeaders;
        
        this.headers = supportedHTTPHeaders.getEnumConstants();
        this.headerCount = null==this.headers? 0 : headers.length;
        
        this.revisions = supportedHTTPRevisions.getEnumConstants();
        this.contentTypes = supportedHTTPContentTypes.getEnumConstants();
        
        //find ordinal values and max length
        int maxVerbLength = 0;
        this.verbs = supportedHTTPVerbs.getEnumConstants();
        if (this.verbs != null) {
	        int j = verbs.length;
	        while (--j >= 0) {        	
	        	maxVerbLength = Math.max(maxVerbLength, verbs[j].name().length());             
	        }
        }
        this.maxVerbLength = maxVerbLength;
        
        //build header lookup trie parser

        assert(false == (trustAccurateStrings=false)); //side effect by design, do not modify
        
        headerParser = new TrieParser(512, 2, trustAccurateStrings, false, true);        
        if (headers != null) {
	        int h = headers.length;
	        while (--h>=0) {	
	        	headerParser.setUTF8Value(headers[h].writingRoot(), headers[h].ordinal());
	        }
        }

    }
  
    public TrieParser headerParser() {
    	return headerParser;
    }
    
	public boolean headerMatches(int headerId, CharSequence cs) {
		return match(cs, headers[headerId].writingRoot());
	}

	public boolean verbMatches(int verbId, CharSequence cs) {
		return match(cs, verbs[verbId].getKey());
	}

	public boolean match(CharSequence a, CharSequence b) {
		if(b.length() != a.length()) {
			return false;
		}
		int i = b.length();
		while (--i>=0) {
			if (Character.toLowerCase(b.charAt(i))!= Character.toLowerCase(a.charAt(i))) {
				return false;
			}
		}
		return true;
	}


    
}
