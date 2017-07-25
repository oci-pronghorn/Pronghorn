package com.ociweb.pronghorn.network.config;

import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

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
	private TrieParser contentTypeTrie;
    
    private static HTTPSpecification<HTTPContentTypeDefaults,HTTPRevisionDefaults,HTTPVerbDefaults,HTTPHeaderDefaults> defaultSpec;
    
    public static HTTPSpecification<HTTPContentTypeDefaults,HTTPRevisionDefaults,HTTPVerbDefaults,HTTPHeaderDefaults>  defaultSpec() {
        if (null == defaultSpec) {
            defaultSpec = new HTTPSpecification(HTTPContentTypeDefaults.class, HTTPRevisionDefaults.class, HTTPVerbDefaults.class,  HTTPHeaderDefaults.class );
        } 
        return defaultSpec;
    }
    
    
	public final IntHashTable headerTable(TrieParserReader localReader) {
		assert(headers!=null) : "check ProGuard it may be removing enums in the build process.";
		IntHashTable headerToPosTable = IntHashTable.newTableExpectingCount(headers.length);		
		int h = headers.length;
		int count = 0;
		while (--h>=0) {
			int ord = headers[h].ordinal();
			boolean ok = IntHashTable.setItem(headerToPosTable, 
					                          HTTPHeader.HEADER_BIT | ord, HTTPHeader.HEADER_BIT | (count++));
			assert(ok);
		}
		return headerToPosTable;
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
        
        this.contentTypeTrie = contentTypeTrieBuilder(contentTypes);
        
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

	public int headerId(byte[] h, TrieParserReader reader) {
		return (int)reader.query(reader, headerParser(), h, 0, h.length, Integer.MAX_VALUE);
	}

	public TrieParser contentTypeTrieBuilder() {
		return contentTypeTrie;
	}

	private TrieParser contentTypeTrieBuilder(HTTPContentType[] types) {
		  int x;
		  TrieParser typeMap = new TrieParser(4096,1,true,false,true);	//TODO: set switch to turn on off the deep check skip     TODO: must be shared across all instances?? 
	      if (null!=types) {
		      x = types.length;
		      while (--x >= 0) {
		    	  //with or without the  charset part on the end
		    	  //Content-Type: text/html; charset=ISO-8859-1
		    	  typeMap.setUTF8Value(types[x].contentType(),"\r\n", types[x].ordinal());	
		    	  typeMap.setUTF8Value(types[x].contentType(),"; charset=ISO-8859-1\r\n", types[x].ordinal());	
		    	  
		    	  typeMap.setUTF8Value(types[x].contentType(),"\n", types[x].ordinal());  //\n must be last because we prefer to have it pick \r\n
		    	  typeMap.setUTF8Value(types[x].contentType(),"; charset=ISO-8859-1\n", types[x].ordinal());
		      }
	      }
		return typeMap;
	}


    
}
