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
    
    
    public final int GET_ID;
    public final int HEAD_ID;
    public final byte[][] revisionBytes; //TODO: caution, code using this may not find it NUMA local
    public final byte[][] contentTypeBytes; //TODO: caution, code using this may not find it NUMA local
    public final int headerCount;
    
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
        
        H[] headers = supportedHTTPHeaders.getEnumConstants();
        headerCount = headers.length;
        
        //populate revision bytes
        R[] revisions = supportedHTTPRevisions.getEnumConstants();
        int r = revisions.length;
        revisionBytes = new byte[r][];
        while (--r >= 0) {
            revisionBytes[revisions[r].ordinal()] = revisions[r].getBytes();
        }
        
        //populate content bytes
        T[] cTypes = supportedHTTPContentTypes.getEnumConstants();
        int t = cTypes.length;
        contentTypeBytes = new byte[t][];
        while (--t >=  0) {
            contentTypeBytes[ cTypes[t].ordinal() ] = (cTypes[t].contentType().toString()+"\n").getBytes();            
        }
        
        //find ordinal values
        V[] verbs = supportedHTTPVerbs.getEnumConstants();
        int j = verbs.length;
        int localGet = 0;
        int localHead = 0;
        while (--j >= 0) {
            if (verbs[j].name().startsWith("GET")) {
                localGet = verbs[j].ordinal();
            } else if (verbs[j].name().startsWith("HEAD")) {
                localHead = verbs[j].ordinal();
            }            
        }
        GET_ID = localGet;
        HEAD_ID = localHead;
        
    }
    
    
    
}
