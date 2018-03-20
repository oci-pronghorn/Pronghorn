package com.ociweb.pronghorn.network.http;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.struct.BStructSchema;
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

    private JSONExtractorCompleted[] requestJSONExtractor = new JSONExtractorCompleted[4];
    
    private final int defaultLength = 4;
    private FieldExtractionDefinitions[] routeDefinitions = new FieldExtractionDefinitions[defaultLength];
    
	private int routeCount = 0;
	private AtomicInteger pathCount = new AtomicInteger();
	    
    public final int END_OF_HEADER_ID;
    public final int UNKNOWN_HEADER_ID;
    
    public final int UNMAPPED_ROUTE =   (1<<((32-2)-HTTPVerb.BITS))-1;//a large constant which fits in the verb field
    
    private URLTemplateParser routeParser;
    private BStructSchema schmea = new BStructSchema();//TODO: inject from elsewhere.
	
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

		String constantUnknownRoute = "${path}";//do not modify
		int groupId = UNMAPPED_ROUTE;//routeCount can not be inc due to our using it to know if there are valid routes.
		int pathId = UNMAPPED_ROUTE;

		this.allHeadersExtraction = routeParser().addPath(constantUnknownRoute, groupId, pathId);
   
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
	        boolean trustText = false; 
			routeParser = new URLTemplateParser(urlMap, trustText);
		}
		URLTemplateParser parser = routeParser;
		return parser;
	}

	void storeRequestExtractionParsers(int idx, FieldExtractionDefinitions route) {
		if (idx>=routeDefinitions.length) {
			int i = routeDefinitions.length;
			FieldExtractionDefinitions[] newArray = new FieldExtractionDefinitions[i*2]; //only grows on startup as needed
			System.arraycopy(routeDefinitions, 0, newArray, 0, i);
			routeDefinitions = newArray;
		}
		routeDefinitions[idx]=route;	
	}

	void storeRequestedHeaders(int idx, IntHashTable headers) {
		
		if (idx>=requestHeaderMask.length) {
			int i = requestHeaderMask.length;
			IntHashTable[] newArray = new IntHashTable[i*2]; //only grows on startup as needed
			System.arraycopy(requestHeaderMask, 0, newArray, 0, i);
			requestHeaderMask = newArray;
		}
		requestHeaderMask[idx]=headers;
	}
	
	
	void storeRequestedJSONMapping(int idx, JSONExtractorCompleted extractor) {
		
		if (idx>=requestJSONExtractor.length) {
			int i = requestJSONExtractor.length;
			JSONExtractorCompleted[] newArray = new JSONExtractorCompleted[i*2]; //only grows on startup as needed
			System.arraycopy(requestJSONExtractor, 0, newArray, 0, i);
			requestJSONExtractor = newArray;
		}
		requestJSONExtractor[idx] = extractor;
	}
	
	public int totalPathsCount() {
		return pathCount.get();
	}	

	public FieldExtractionDefinitions extractionParser(int routeId) {
		return routeId<routeDefinitions.length ? routeDefinitions[routeId] : allHeadersExtraction;
	}

	public int headerCount(int routeId) {
		return IntHashTable.count(headerToPositionTable(routeId));
	}

	public IntHashTable headerToPositionTable(int routeId) {
		assert(null!=allHeadersTable);
		return routeId<requestHeaderMask.length && (null!=requestHeaderMask[routeId]) ? 
				           requestHeaderMask[routeId] : allHeadersTable;
	}

	public JSONExtractorCompleted JSONExtractor(int routeId) {
		return routeId<requestJSONExtractor.length ? requestJSONExtractor[routeId] : null;
	}

    @Override
	public HTTPSpecification httpSpec() {
		return httpSpec;
	}

    public int headerId(byte[] h) {
    	return httpSpec.headerId(h, localReader);
    }
    

	public CompositeRoute registerCompositeRoute(HTTPHeader ... headers) {

		URLTemplateParser parser = routeParser();
		IntHashTable headerTable = HeaderUtil.headerTable(localReader, httpSpec, headers);
		
		return new CompositeRouteImpl(schmea, this, null, parser, headerTable, headers, routeCount++, pathCount);
	}


	public CompositeRoute registerCompositeRoute(JSONExtractorCompleted extractor, HTTPHeader ... headers) {

		URLTemplateParser parser = routeParser();
		IntHashTable headerTable = HeaderUtil.headerTable(localReader, httpSpec, headers);
		
		return new CompositeRouteImpl(schmea, this, extractor, parser, headerTable, headers, routeCount++, pathCount);
	}

	public boolean appendPipeIdMappingForAllGroupIds(
            Pipe<HTTPRequestSchema> pipe, 
            int p, 
            ArrayList<Pipe<HTTPRequestSchema>>[][] collectedHTTPRequstPipes) {
		
			assert(null!=collectedHTTPRequstPipes);
			boolean added = false;
			int i = routeCount;
			if (i==0) {
				added  = true;
				collectedHTTPRequstPipes[p][0].add(pipe); //ALL DEFAULT IN A SINGLE ROUTE
			} else {
				while (--i>=0) {
					added  = true;
					if (null != routeDefinitions[i] 
						&& UNMAPPED_ROUTE!=routeDefinitions[i].pathId
					   ) {
						assert(null != collectedHTTPRequstPipes[p][routeDefinitions[i].pathId]);
						collectedHTTPRequstPipes[p][routeDefinitions[i].pathId].add(pipe);
					}
				}
			}
			return added;
	}
	
	public boolean appendPipeIdMappingForIncludedGroupIds(
			                      Pipe<HTTPRequestSchema> pipe, 
			                      int p, 
			                      ArrayList<Pipe<HTTPRequestSchema>>[][] collectedHTTPRequstPipes,
			                      int ... groupsIds) {
		boolean added = false;
		int i = routeDefinitions.length;
		while (--i>=0) {
			if (null!=routeDefinitions[i]) {
				if (contains(groupsIds, routeDefinitions[i].groupId)) {	
					added = true;
					collectedHTTPRequstPipes[p][routeDefinitions[i].pathId].add(pipe);	
				}
			}
		}
			
		return added;
	}
	
	public boolean appendPipeIdMappingForExcludedGroupIds(
            Pipe<HTTPRequestSchema> pipe, 
            int p, 
            ArrayList<Pipe<HTTPRequestSchema>>[][] collectedHTTPRequstPipes,
            int ... groupsIds) {
			boolean added = false;
			int i = routeDefinitions.length;
			while (--i>=0) {
				if (null!=routeDefinitions[i]) {
					if (!contains(groupsIds, routeDefinitions[i].groupId)) {			
						added = true;
						collectedHTTPRequstPipes[p][routeDefinitions[i].pathId].add(pipe);	
					}
				}
			}
			return added;
	}

	private boolean contains(int[] groupsIds, int groupId) {
		int i = groupsIds.length;
		while (--i>=0) {
			if (groupId == groupsIds[i]) {
				return true;
			}
		}
		return false;
	}
	
	
	
	
	
	

	
	
	
}
