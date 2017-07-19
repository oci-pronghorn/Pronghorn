package com.ociweb.pronghorn.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTP1xRouterStageConfig<T extends Enum<T> & HTTPContentType,
                                    R extends Enum<R> & HTTPRevision,
                                    V extends Enum<V> & HTTPVerb,
									H extends Enum<H> & HTTPHeader> implements RouterStageConfig {
	
	public final HTTPSpecification<T,R,V,H> httpSpec;
	
    public final TrieParser urlMap;
    public final TrieParser verbMap;
    public final TrieParser revisionMap;
    public final TrieParser headerMap;
    
    public static final Logger logger = LoggerFactory.getLogger(HTTP1xRouterStageConfig.class);
    
    private IntHashTable[] requestHeaderMask = new IntHashTable[4];
    private TrieParser[] requestHeaderParser = new TrieParser[4];
    
    private final int defaultLength = 4;
    private byte[][] requestExtractions = new byte[defaultLength][];
    private FieldExtractionDefinitions[] routeDefinitions = new FieldExtractionDefinitions[defaultLength];
    
	private int routesCount = 0;
    
    public final int END_OF_HEADER_ID;
    public final int UNKNOWN_HEADER_ID;
    public static final int UNMAPPED_ROUTE =   (1<<((32-2)-HTTPVerb.BITS))-1;//a large constant which fits in the verb field
   
    private URLTemplateParser routeParser;
	
	private final TrieParserReader localReader = new TrieParserReader(2, true);

	private IntHashTable allHeadersTable;
	private FieldExtractionDefinitions allHeadersExtraction;
	

	public HTTP1xRouterStageConfig(HTTPSpecification<T,R,V,H> httpSpec) {
		this.httpSpec = httpSpec;

        this.revisionMap = new TrieParser(256,true); //avoid deep check        
        //Load the supported HTTP revisions
        R[] revs = (R[])httpSpec.supportedHTTPRevisions.getEnumConstants();
        if (revs != null) {
	        int z = revs.length;               
	        while (--z >= 0) {
	        	revisionMap.setUTF8Value(revs[z].getKey(), "\r\n", revs[z].ordinal());
	            revisionMap.setUTF8Value(revs[z].getKey(), "\n", revs[z].ordinal()); //\n must be last because we prefer to have it pick \r\n          
	        }
        }
        
        this.verbMap = new TrieParser(256,false);//does deep check
        //logger.info("building verb map");
        //Load the supported HTTP verbs
        V[] verbs = (V[])httpSpec.supportedHTTPVerbs.getEnumConstants();
        if (verbs != null) {
	        int y = verbs.length;
	        assert(verbs.length>=1) : "only found "+verbs.length+" defined";
	        while (--y >= 0) {
	        	//logger.info("add verb {}",verbs[y].getKey());
	            verbMap.setUTF8Value(verbs[y].getKey()," ", verbs[y].ordinal());           
	        }
        }
        END_OF_HEADER_ID  = httpSpec.headerCount+2;//for the empty header found at the bottom of the header
        UNKNOWN_HEADER_ID = httpSpec.headerCount+1;

        this.headerMap = new TrieParser(2048,2,false,true,true);//do not skip deep checks, we do not know which new headers may appear.

        headerMap.setUTF8Value("\r\n", END_OF_HEADER_ID);
        headerMap.setUTF8Value("\n", END_OF_HEADER_ID);  //\n must be last because we prefer to have it pick \r\n
    
        //Load the supported header keys
        H[] shr =  (H[])httpSpec.supportedHTTPHeaders.getEnumConstants();
        if (shr != null) {
	        int w = shr.length;
	        while (--w >= 0) {
	            //must have tail because the first char of the tail is required for the stop byte
	            headerMap.setUTF8Value(shr[w].readingTemplate(), "\r\n",shr[w].ordinal());
	            headerMap.setUTF8Value(shr[w].readingTemplate(), "\n",shr[w].ordinal()); //\n must be last because we prefer to have it pick \r\n
	        }     
        }
        //unknowns are the least important and must be added last 
        this.urlMap = new TrieParser(512,2,false //never skip deep check so we can return 404 for all "unknowns"
        	 	                   ,true,true);  
   
        this.allHeadersTable = httpSpec.headerTable(localReader);        
        boolean trustText = false; 
		String constantUnknownRoute = "${path}";//do not modify
		this.allHeadersExtraction = routeParser().addRoute(constantUnknownRoute, UNMAPPED_ROUTE, urlMap, trustText);
		storeRequestExtractionParsers(allHeadersExtraction);
		storeRequestedExtractions(urlMap.lastSetValueExtractonPattern());
        
        headerMap.setUTF8Value("%b: %b\r\n", UNKNOWN_HEADER_ID);        
        headerMap.setUTF8Value("%b: %b\n", UNKNOWN_HEADER_ID); //\n must be last because we prefer to have it pick \r\n
       
	}

		
	public void debugURLMap() {
		
		String actual = urlMap.toDOT(new StringBuilder()).toString();
		
		System.err.println(actual);
		
	}



	private URLTemplateParser routeParser() {
		//Many projects do not need this so do not build..
		if (routeParser==null) {
			routeParser = new URLTemplateParser();
		}
		URLTemplateParser parser = routeParser;
		return parser;
	}

	private void storeRequestExtractionParsers(FieldExtractionDefinitions route) {
		if (routesCount>=routeDefinitions.length) {
			int i = routeDefinitions.length;
			FieldExtractionDefinitions[] newArray = new FieldExtractionDefinitions[i*2]; //only grows on startup as needed
			System.arraycopy(routeDefinitions, 0, newArray, 0, i);
			routeDefinitions = newArray;
		}
		routeDefinitions[routesCount]=route;	
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
	
	private void storeRequestedHeaderParser(TrieParser headers) {
		
		if (routesCount>=requestHeaderParser.length) {
			int i = requestHeaderParser.length;
			TrieParser[] newArray = new TrieParser[i*2]; //only grows on startup as needed
			System.arraycopy(requestHeaderParser, 0, newArray, 0, i);
			requestHeaderParser = newArray;
		}
		requestHeaderParser[routesCount]=headers;
	}
	
	public int routesCount() {
		return routesCount;
	}	

	public FieldExtractionDefinitions extractionParser(int routeId) {
		return routeId<routeDefinitions.length ? routeDefinitions[routeId] : allHeadersExtraction;
	}

	public int headerCount(int routeId) {
		return IntHashTable.count(headerToPositionTable(routeId));
	}

	public IntHashTable headerToPositionTable(int routeId) {
		return routeId<requestHeaderMask.length ? requestHeaderMask[routeId] : allHeadersTable;
	}

	public TrieParser headerTrieParser(int routeId) {
		return routeId<requestHeaderParser.length ? requestHeaderParser[routeId] : httpSpec.headerParser();
	}

    private int registerRoute(CharSequence route, IntHashTable headers, TrieParser headerParser) {

		boolean trustText = false; 
		URLTemplateParser parser = routeParser();
		storeRequestExtractionParsers(parser.addRoute(route, routesCount, urlMap, trustText));
		storeRequestedExtractions(urlMap.lastSetValueExtractonPattern());

		storeRequestedHeaders(headers);
		storeRequestedHeaderParser(headerParser);
		
		int pipeIdx = routesCount;
		routesCount++;
		return pipeIdx;
	}

    public int headerId(byte[] h) {
    	return httpSpec.headerId(h, localReader);
    }


	public int registerRoute(CharSequence route, byte[] ... headers) {
		return registerRoute(route, HeaderUtil.headerTable(localReader, httpSpec, headers), httpSpec.headerParser());
	}

	@Override
	public HTTPSpecification httpSpec() {
		return httpSpec;
	}

	
	
	
}
