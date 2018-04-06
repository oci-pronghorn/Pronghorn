package com.ociweb.pronghorn.network.http;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructTypes;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTP1xRouterStageConfig<T extends Enum<T> & HTTPContentType,
                                    R extends Enum<R> & HTTPRevision,
                                    V extends Enum<V> & HTTPVerb,
									H extends Enum<H> & HTTPHeader> implements RouterStageConfig {
	
	public static final Logger logger = LoggerFactory.getLogger(HTTP1xRouterStageConfig.class);

	public final HTTPSpecification<T,R,V,H> httpSpec;
	
    public final TrieParser urlMap;
    public final TrieParser verbMap;
    public final TrieParser revisionMap;
      
    private final int defaultLength = 4;
    
    private TrieParser[] headersParser = new TrieParser[4];    
    private JSONExtractorCompleted[] requestJSONExtractor = new JSONExtractorCompleted[defaultLength];    
    private FieldExtractionDefinitions[] pathDefinitions = new FieldExtractionDefinitions[defaultLength];
    
	private int routeCount = 0;
	private AtomicInteger pathCount = new AtomicInteger();
 
    
    final int UNMAPPED_ROUTE =   (1<<((32-2)-HTTPVerb.BITS))-1;//a large constant which fits in the verb field
    public final int UNMAPPED_STRUCT; 
    final TrieParser unmappedHeaders;
    public final long unmappedPathField;
    public int[] unmappedIndexPos;
    
    
    private URLTemplateParser routeParser;
    private final StructRegistry userStructs;
	
	private final TrieParserReader localReader = new TrieParserReader(2, true);


	public int totalSizeOfIndexes(int structId) {
		return userStructs.totalSizeOfIndexes(structId);
	}
	
	public <T extends Object> T getAssociatedObject(long field) {
		return userStructs.getAssociatedObject(field);
	}
	
	public HTTP1xRouterStageConfig(HTTPSpecification<T,R,V,H> httpSpec, StructRegistry userStructs) {
		this.httpSpec = httpSpec;
		this.userStructs = userStructs;
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


        //unknowns are the least important and must be added last 
        this.urlMap = new TrieParser(512,2,false //never skip deep check so we can return 404 for all "unknowns"
        	 	                   ,true,true);
        
		String constantUnknownRoute = "${path}";//do not modify
		int routeId = UNMAPPED_ROUTE;//routeCount can not be inc due to our using it to know if there are valid routes.
		int pathId = UNMAPPED_ROUTE;
				
		int structId = HTTPUtil.newHTTPStruct(userStructs);
		unmappedPathField = userStructs.growStruct(structId,StructTypes.Text,0,"path".getBytes());				
		
		unmappedIndexPos = new int[] {StructRegistry.FIELD_MASK&(int)unmappedPathField};
		
		routeParser().addPath(constantUnknownRoute, routeId, pathId, structId);
		UNMAPPED_STRUCT = structId;
		
		unmappedHeaders = HTTPUtil.buildHeaderParser(
				userStructs, 
    			structId,
    			HTTPHeaderDefaults.CONTENT_LENGTH,
    			HTTPHeaderDefaults.TRANSFER_ENCODING,
    			HTTPHeaderDefaults.CONNECTION
			);
		
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
		return routeParser;
	}

	public void storeRouteHeaders(int routeId, TrieParser headerParser) {
		if (routeId>=headersParser.length) {
			int i = headersParser.length;
			TrieParser[] newArray = new TrieParser[i*2];
			System.arraycopy(headersParser, 0, newArray, 0, i);
			headersParser = newArray;
		}
		headersParser[routeId]=headerParser;
	}
	
	void storeRequestExtractionParsers(int pathIdx, FieldExtractionDefinitions route) {
		
		//////////store for lookup by path
		if (pathIdx>=pathDefinitions.length) {
			int i = pathDefinitions.length;
			FieldExtractionDefinitions[] newArray = new FieldExtractionDefinitions[i*2]; //only grows on startup as needed
			System.arraycopy(pathDefinitions, 0, newArray, 0, i);
			pathDefinitions = newArray;
		}
		pathDefinitions[pathIdx]=route;	
		
		//we have 1 pipe per composite route so nothing gets stuck and
		//we have max visibility into the traffic by route type.
		//any behavior can process multiple routes but it comes in as multiple pipes.
		//each pipe can however send multiple different routes if needed since each 
		//message contains its own structId
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

	public int getRouteIdForPathId(int pathId) {
		return (pathId != UNMAPPED_ROUTE) ? extractionParser(pathId).routeId : -1;
	}

	//only needed on startup, ok to be linear search
	public int getStructIdForRouteId(final int routeId) {
		
		int result = -1;
		if (routeId != UNMAPPED_ROUTE) {
			int i = pathDefinitions.length;
			while (--i>=0) {
				
				if ((pathDefinitions[i]!=null) && (routeId == pathDefinitions[i].routeId)) {
					if (result==-1) {
						result = pathDefinitions[i].structId;
					} else {
						assert(result == pathDefinitions[i].structId) : "route may only have 1 structure, found more";					
					}
				}
			}
			if (-1 == result) {
				throw new UnsupportedOperationException("Unable to find routeId "+routeId);			
			}
		} else {
			result = UNMAPPED_STRUCT;
		}
		return result;
	}
	
	public FieldExtractionDefinitions extractionParser(int pathId) {
		return pathDefinitions[pathId];
	}
	
	public TrieParser headerParserRouteId(int routeId) {
		return headersParser[routeId];		
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

		return new CompositeRouteImpl(userStructs, this, null, routeParser(), headers, routeCount++, pathCount);
	}


	public CompositeRoute registerCompositeRoute(JSONExtractorCompleted extractor, HTTPHeader ... headers) {

		return new CompositeRouteImpl(userStructs, this, extractor, routeParser(), headers, routeCount++, pathCount);
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
					if (null != pathDefinitions[i] 
						&& UNMAPPED_ROUTE!=pathDefinitions[i].pathId
					   ) {
						assert(null != collectedHTTPRequstPipes[p][pathDefinitions[i].pathId]);
						collectedHTTPRequstPipes[p][pathDefinitions[i].pathId].add(pipe);
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
		int i = pathDefinitions.length;
		while (--i>=0) {
			if (null!=pathDefinitions[i]) {
				if (contains(groupsIds, pathDefinitions[i].routeId)) {	
					added = true;
					collectedHTTPRequstPipes[p][pathDefinitions[i].pathId].add(pipe);	
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
			int i = pathDefinitions.length;
			while (--i>=0) {
				if (null!=pathDefinitions[i]) {
					if (!contains(groupsIds, pathDefinitions[i].routeId)) {			
						added = true;
						collectedHTTPRequstPipes[p][pathDefinitions[i].pathId].add(pipe);	
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

	public int[] paramIndexArray(int pathId) {
		return pathDefinitions[pathId].paramIndexArray();
	}

	public int totalRoutesCount() {
		return routeCount;
	}

}
