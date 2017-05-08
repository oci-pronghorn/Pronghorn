package com.ociweb.pronghorn.network.http;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeaderKey;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
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
    
    private IntHashTable[] requestHeaderMask = new IntHashTable[4];
    
    private final int defaultLength = 4;
    private byte[][] requestExtractions = new byte[defaultLength][];
    private TrieParser[] requestExtractionParsers = new TrieParser[defaultLength];
    
	private int routesCount = 0;
    
    public final int END_OF_HEADER_ID;
    public final int UNKNOWN_HEADER_ID;
	
    private final URLTemplateParser templateParser = new URLTemplateParser();

	public HTTP1xRouterStageConfig(HTTPSpecification<T,R,V,H> httpSpec) {
		this.httpSpec = httpSpec;

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
                
        END_OF_HEADER_ID  = httpSpec.headerCount+2;//for the empty header found at the bottom of the header
        UNKNOWN_HEADER_ID = httpSpec.headerCount+1;

        this.headerMap = new TrieParser(2048,false);//do not skip deep checks, we do not know which new headers may appear.
        
        //TODO: if the client should write extra data after the first GET/PUT this will be assumed as part of the folloing message and cause parse issues.
        //      when client was adding extra line feed we hacked this to add headerMap.setUTF8Value("\r\n\r\n", END_OF_HEADER_ID); but that was a bad fix for one special case
        //      we should think of a more general solution 

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
                
        this.urlMap = new TrieParser(512,1,true,true,true);       
        
	}
	
	
	public void debugURLMap() {
		
		String actual = urlMap.toDOT(new StringBuilder()).toString();
		
		System.err.println(actual);
		
	}

   public int registerRoute(CharSequence route, IntHashTable headers) {
		
		boolean trustText = false; 
		TrieParser routeExtractionParser = templateParser.addRoute(route, routesCount, urlMap, trustText);

		storeRequestExtractionParsers(routeExtractionParser);
		storeRequestedExtractions(urlMap.lastSetValueExtractonPattern());

		storeRequestedHeaders(headers);
		int pipeIdx = routesCount;
		routesCount++;
		return pipeIdx;
	}

	private void storeRequestExtractionParsers(TrieParser routeExtractionParser) {
		if (routesCount>=requestExtractionParsers.length) {
			int i = requestExtractionParsers.length;
			TrieParser[] newArray = new TrieParser[i*2]; //only grows on startup as needed
			System.arraycopy(requestExtractionParsers, 0, newArray, 0, i);
			requestExtractionParsers = newArray;
		}
		requestExtractionParsers[routesCount]=routeExtractionParser;		
	}

	private void storeRequestedExtractions(byte[] lastSetValueExtractonPattern) {
		if (routesCount>=requestExtractions.length) {
			int i = requestExtractions.length;
			byte[][] newArray = new byte[i*2][]; //only grows on startup as needed
			System.arraycopy(requestExtractions, 0, newArray, 0, i);
			requestExtractions = newArray;
		}
		requestExtractions[routesCount]=lastSetValueExtractonPattern;		
	}


	private void storeRequestedHeaders(IntHashTable headers) {
		
		if (routesCount>=requestHeaderMask.length) {
			int i = requestHeaderMask.length;
			IntHashTable[] newArray = new IntHashTable[i*2]; //only grows on startup as needed
			System.arraycopy(requestHeaderMask, 0, newArray, 0, i);
			requestHeaderMask = newArray;
		}
		requestHeaderMask[routesCount]=headers;
	}
	
	public int routesCount() {
		return routesCount;
	}	

	public TrieParser extractionParser(int idx) {
		return requestExtractionParsers[idx];
	}

	public int headerCount(int routeId) {
		return IntHashTable.count(headerToPositionTable(routeId));
	}

	public IntHashTable headerToPositionTable(int routeId) {
		return requestHeaderMask[routeId];
	}


	
	
	
}
