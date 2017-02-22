package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeaderKey;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.util.TrieParser;

public class HTTP1xRouterStageConfig<T extends Enum<T> & HTTPContentType,
                                    R extends Enum<R> & HTTPRevision,
                                    V extends Enum<V> & HTTPVerb,
									H extends Enum<H> & HTTPHeaderKey> {
	
	public final HTTPSpecification<T,R,V,H> httpSpec;	
    public final TrieParser urlMap;
    public final TrieParser verbMap;
    public final TrieParser revisionMap;
    public final TrieParser headerMap;
    public final long[] requestHeaderMask;

    
    public final int END_OF_HEADER_ID;
    public final int UNKNOWN_HEADER_ID;
	
	public HTTP1xRouterStageConfig(CharSequence[] paths, long[] headers, HTTPSpecification<T,R,V,H> httpSpec) {
		this.httpSpec = httpSpec;
		this.requestHeaderMask = headers;

        this.revisionMap = new TrieParser(256,true); //avoid deep check        
        //Load the supported HTTP revisions
        R[] revs = (R[])httpSpec.supportedHTTPRevisions.getEnumConstants();
        int z = revs.length;               
        while (--z >= 0) {
        	revisionMap.setUTF8Value(revs[z].getKey(), "\r\n", revs[z].ordinal());
            revisionMap.setUTF8Value(revs[z].getKey(), "\n", revs[z].ordinal()); //\n must be last because we prefer to have it pick \r\n          
        }
        
        
        
        this.verbMap = new TrieParser(256,false);//does deep check
        //Load the supported HTTP verbs
        V[] verbs = (V[])httpSpec.supportedHTTPVerbs.getEnumConstants();
        int y = verbs.length;
        while (--y >= 0) {
            verbMap.setUTF8Value(verbs[y].getKey()," ", verbs[y].ordinal());           
        }
        
        
        //load all the routes
        this.urlMap = new TrieParser(1024,1,true,true,true);
        int x = paths.length;
        while (--x>=0) {

            int b;
            int value = x;
            if (' '==paths[x].charAt(paths[x].length()-1)) {
                b=urlMap.setUTF8Value(paths[x], value);
            } else {
                b=urlMap.setUTF8Value(paths[x], " ",value);
            }
        }
                
        END_OF_HEADER_ID  = httpSpec.headerCount+2;//for the empty header found at the bottom of the header
        UNKNOWN_HEADER_ID = httpSpec.headerCount+1;

        this.headerMap = new TrieParser(2048,false);//do not skip deep checks, we do not know which new headers may appear.
        headerMap.setUTF8Value("\r\n", END_OF_HEADER_ID);
        headerMap.setUTF8Value("\n", END_OF_HEADER_ID);  //\n must be last because we prefer to have it pick \r\n
    
        //Load the supported header keys
        H[] shr =  (H[])httpSpec.supportedHTTPHeaders.getEnumConstants();
        int w = shr.length;
        while (--w >= 0) {
            //must have tail because the first char of the tail is required for the stop byte
            headerMap.setUTF8Value(shr[w].getKey(), "\r\n",shr[w].ordinal());
            headerMap.setUTF8Value(shr[w].getKey(), "\n",shr[w].ordinal()); //\n must be last because we prefer to have it pick \r\n
        }     
        //unknowns are the least important and must be added last 
        headerMap.setUTF8Value("%b: %b\r\n", UNKNOWN_HEADER_ID);        
        headerMap.setUTF8Value("%b: %b\n", UNKNOWN_HEADER_ID); //\n must be last because we prefer to have it pick \r\n
                
	}
	
	
	
}
