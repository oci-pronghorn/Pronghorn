package com.ociweb.pronghorn.stage.network.config;

public class HTTPSpecification  <   T extends Enum<T> & HTTPContentType,
                                    R extends Enum<R> & HTTPRevision,
                                    V extends Enum<V> & HTTPVerb,
                                    H extends Enum<H> & HTTPHeaderKey
                                    > {
    
    public final Class<T> supportedHTTPContentTypes;
    public final Class<R> supportedHTTPRevisions;
    public final Class<V> supportedHTTPVerbs;
    public final Class<H> supportedHTTPHeaders;
    
    public final int maxVerbLength;
    public final int maxRevisionLength;

    @Deprecated
    public final byte[][] revisionBytes; //TODO: caution, code using this may not find it NUMA local
    @Deprecated
    public final byte[][] contentTypeBytes; //TODO: caution, code using this may not find it NUMA local
    
    public final int headerCount;
    public final H[] headers;
    public final T[] contentTypes;
    
    private static HTTPSpecification<HTTPContentTypeDefaults,HTTPRevisionDefaults,HTTPVerbDefaults,HTTPHeaderKeyDefaults> defaultSpec;
    
    public static HTTPSpecification<HTTPContentTypeDefaults,HTTPRevisionDefaults,HTTPVerbDefaults,HTTPHeaderKeyDefaults>  defaultSpec() {
        if (null == defaultSpec) {
            defaultSpec = new HTTPSpecification(HTTPContentTypeDefaults.class, HTTPRevisionDefaults.class, HTTPVerbDefaults.class,  HTTPHeaderKeyDefaults.class );
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
        
        //populate revision bytes
        R[] revisions = supportedHTTPRevisions.getEnumConstants();
        int r = revisions.length;
        revisionBytes = new byte[r][];
        int maxRevisionLength = 0;
        while (--r >= 0) {
            byte[] supportedRevisionBytes = revisions[r].getBytes();
            maxRevisionLength = Math.max(maxRevisionLength, supportedRevisionBytes.length);
			revisionBytes[revisions[r].ordinal()] = supportedRevisionBytes;
        }
        this.maxRevisionLength = maxRevisionLength;
        
        //populate content bytes
        contentTypes = supportedHTTPContentTypes.getEnumConstants();
        int t = contentTypes.length;
        contentTypeBytes = new byte[t][];
        while (--t >=  0) {
            contentTypeBytes[ contentTypes[t].ordinal() ] = (contentTypes[t].contentType().toString()+"\n").getBytes();            
        }
        
        //find ordinal values and max length
        int maxVerbLength = 0;
        V[] verbs = supportedHTTPVerbs.getEnumConstants();
        int j = verbs.length;
        int localGet = 0;
        int localHead = 0;
        while (--j >= 0) {
        	
        	String name = verbs[j].name();
        	maxVerbLength = Math.max(maxVerbLength, name.length());
        	
            if (name.startsWith("GET")) {
                localGet = verbs[j].ordinal();
            } else if (name.startsWith("HEAD")) {
                localHead = verbs[j].ordinal();
            }            
        }
        this.maxVerbLength = maxVerbLength;

    }

	public boolean headerMatches(int headerId, CharSequence cs) {
		return headers[headerId].getKey().equals(cs);
	}
    
    
    
}
