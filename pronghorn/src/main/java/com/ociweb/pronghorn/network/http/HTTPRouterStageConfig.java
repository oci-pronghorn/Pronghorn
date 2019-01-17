package com.ociweb.pronghorn.network.http;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.json.encode.JSONObject;
import com.ociweb.json.encode.JSONRenderer;
import com.ociweb.pronghorn.network.ServerConnectionStruct;
import com.ociweb.pronghorn.network.config.HTTPContentType;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructType;
import com.ociweb.pronghorn.util.AppendableBuilder;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTPRouterStageConfig<T extends Enum<T> & HTTPContentType,
                                    R extends Enum<R> & HTTPRevision,
                                    V extends Enum<V> & HTTPVerb,
									H extends Enum<H> & HTTPHeader> implements RouterStageConfig {
	
	public static final Logger logger = LoggerFactory.getLogger(HTTPRouterStageConfig.class);

	public final HTTPSpecification<T,R,V,H> httpSpec;
	
    public final TrieParser urlMap;
    public final TrieParser verbMap;
    public final TrieParser revisionMap;
      
    private final int defaultLength = 4;
    
    private TrieParser[] headersParser = new TrieParser[4];    
    private JSONExtractorCompleted[] requestJSONExtractorForPath = new JSONExtractorCompleted[defaultLength];    
    private FieldExtractionDefinitions[] pathToRoute = new FieldExtractionDefinitions[defaultLength];
    
	private int routeCount = 0;
	private AtomicInteger pathCount = new AtomicInteger();
 
    
    final int UNMAPPED_ROUTE =   (1<<((32-2)-HTTPVerb.BITS))-1;//a large constant which fits in the verb field
    public final int UNMAPPED_STRUCT; 
    final TrieParser unmappedHeaders;
    public final long unmappedPathField;
    public int[] unmappedIndexPos;

	private IntHashTable routeIdTable = new IntHashTable(3);
	
    
    private URLTemplateParser routeParser;
    private final ServerConnectionStruct conStruct;
	
	private final TrieParserReader localReader = new TrieParserReader(true);


	public int totalSizeOfIndexes(int structId) {
		return conStruct.registry.totalSizeOfIndexes(structId);
	}
	
	public <O extends Object> O getAssociatedObject(long field) {
		return conStruct.registry.getAssociatedObject(field);
	}
	
	public HTTPRouterStageConfig(HTTPSpecification<T,R,V,H> httpSpec, 
									ServerConnectionStruct conStruct) {
		this.httpSpec = httpSpec;
		this.conStruct = conStruct;
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
        
        this.verbMap = new TrieParser(256,true);//skip deep check
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
				
		int structId = HTTPUtil.newHTTPStruct(conStruct.registry);
		unmappedPathField = conStruct.registry.growStruct(structId,StructType.Text,0,"path".getBytes());				
		conStruct.registry.setAssociatedObject(unmappedPathField, "path");
		
		unmappedIndexPos = new int[] {StructRegistry.FIELD_MASK&(int)unmappedPathField};
		
		routeParser().addPath(constantUnknownRoute, routeId, pathId, structId);
		UNMAPPED_STRUCT = structId;
		
		unmappedHeaders = HTTPUtil.buildHeaderParser(
				conStruct.registry, 
    			structId,
    			HTTPHeaderDefaults.CONTENT_LENGTH,
    			HTTPHeaderDefaults.TRANSFER_ENCODING,
    			HTTPHeaderDefaults.CONNECTION
			);
		
	}

		
	public void debugURLMap() {
		
		try {
			urlMap.toDOTFile(File.createTempFile("debugTrie", ".dot"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
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
		if (pathIdx>=pathToRoute.length) {
			int i = pathToRoute.length;
			FieldExtractionDefinitions[] newArray = new FieldExtractionDefinitions[i*2]; //only grows on startup as needed
			System.arraycopy(pathToRoute, 0, newArray, 0, i);
			pathToRoute = newArray;
		}
		pathToRoute[pathIdx]=route;	

		//we have 1 pipe per composite route so nothing gets stuck and
		//we have max visibility into the traffic by route type.
		//any behavior can process multiple routes but it comes in as multiple pipes.
		//each pipe can however send multiple different routes if needed since each 
		//message contains its own structId
	}

	void storeRequestedJSONMapping(int routeId, JSONExtractorCompleted extractor) {
		
		if (routeId>=requestJSONExtractorForPath.length) {
			int i = requestJSONExtractorForPath.length;
			JSONExtractorCompleted[] newArray = new JSONExtractorCompleted[i*2]; //only grows on startup as needed
			System.arraycopy(requestJSONExtractorForPath, 0, newArray, 0, i);
			requestJSONExtractorForPath = newArray;
		}
		assert(null==requestJSONExtractorForPath[routeId] || extractor==requestJSONExtractorForPath[routeId]);
		requestJSONExtractorForPath[routeId] = extractor;
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
			int i = pathToRoute.length;
			while (--i>=0) {
				
				if ((pathToRoute[i]!=null) && (routeId == pathToRoute[i].routeId)) {
					if (result==-1) {
						result = pathToRoute[i].structId;
					} else {
						assert(result == pathToRoute[i].structId) : "route may only have 1 structure, found more";					
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
		return pathToRoute[pathId];
	}
	
	public TrieParser headerParserRouteId(int routeId) {
		return headersParser[routeId];		
	}

	public JSONExtractorCompleted JSONExtractor(int routeId) {
		return routeId<requestJSONExtractorForPath.length ? requestJSONExtractorForPath[routeId] : null;
	}

    @Override
	public HTTPSpecification<T,R,V,H> httpSpec() {
		return httpSpec;
	}

    public int headerId(byte[] h) {
    	return httpSpec.headerId(h, localReader);
    }
    

	public CompositeRoute registerCompositeRoute(HTTPHeader ... headers) {

		return new CompositeRouteImpl(conStruct, this, null, routeParser(), headers, routeCount++, pathCount);
	}


	public CompositeRoute registerCompositeRoute(JSONExtractorCompleted extractor, HTTPHeader ... headers) {

		return new CompositeRouteImpl(conStruct, this, extractor, routeParser(), headers, routeCount++, pathCount);
	}

	
	public boolean appendPipeIdMappingForIncludedGroupIds(
			                      Pipe<HTTPRequestSchema> pipe, 
			                      int track, 
			                      ArrayList<Pipe<HTTPRequestSchema>>[][] collectedHTTPRequstPipes,
			                      int ... routeId) {
		boolean added = false;
		
		for(int pathId = 0; pathId<pathToRoute.length; pathId++) {			
			if (null!=pathToRoute[pathId]) {
				//if this pathId belongs to one of this pipes routes then add it.
				if (contains(routeId, pathToRoute[pathId].routeId)) {	
					added = true;
				    ArrayList<Pipe<HTTPRequestSchema>> targetList = collectedHTTPRequstPipes[track][pathToRoute[pathId].routeId];
				    
				    //if this pipe is already in the list for this route do not add it again.
				    if (!targetList.contains(pipe)) {
				    	targetList.add(pipe);
				    }
				}
			} else {
				
				assert(assertRestNull(pathId));
				
				break;//nothing will be found past the null
			}
			
		}
			
		return added;
	}

	private boolean assertRestNull(int pathId) {
		for(int p = pathId; p<pathToRoute.length; p++) {
			if (null!=pathToRoute[pathId]) {
				return false;
			};
		}
		return true;
	}
	
	public boolean appendPipeIdMappingForExcludedGroupIds(
            Pipe<HTTPRequestSchema> pipe, 
            int track, 
            ArrayList<Pipe<HTTPRequestSchema>>[][] collectedHTTPRequstPipes,
            int ... groupsIds) {
			boolean added = false;
			for(int pathId = 0; pathId<pathToRoute.length; pathId++) {	
				if (null!=pathToRoute[pathId]) {
					if (!contains(groupsIds, pathToRoute[pathId].routeId)) {			
						added = true;
						ArrayList<Pipe<HTTPRequestSchema>> targetList = collectedHTTPRequstPipes[track][pathToRoute[pathId].routeId];
						if (!targetList.contains(pipe)) {
							targetList.add(pipe);	
						}
					}
				} else {
					
					assert(assertRestNull(pathId));
					
					break;//nothing will be found past the null
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
		return pathToRoute[pathId].paramIndexArray();
	}
	
	public Object[] paramIndexArrayValidator(int pathId) {
		return pathToRoute[pathId].paramIndexArrayValidator();
	}
	
	

	public int totalRoutesCount() {
		return routeCount;
	}

	public int lookupRouteIdByIdentity(Object associatedObject) {
		
		final int hash = associatedObject.hashCode();
		final int idx = IntHashTable.getItem(routeIdTable, hash);
		if (0==idx) {
			if (!IntHashTable.hasItem(routeIdTable, hash)) {
				throw new UnsupportedOperationException("Object not found: "+associatedObject);			
			}
		}
		return idx;
	}

	public void registerRouteAssociation(int routeId, Object associatedObject) {

		int key = associatedObject.hashCode();
	
		assert(!IntHashTable.hasItem(routeIdTable, key)) : "These objects are too similar or was attached twice, Hash must be unique. Choose different objects";
		if (IntHashTable.hasItem(routeIdTable, key)) {
			logger.warn("Unable to add object {} as an association, Another object with an identical Hash is already held. Try a different object.", associatedObject);		
			return;
		}
		if (!IntHashTable.setItem(routeIdTable, key, routeId)) {
			routeIdTable = IntHashTable.doubleSize(routeIdTable);			
			if (!IntHashTable.setItem(routeIdTable, key, routeId)) {
				throw new RuntimeException("internal error");
			};
		}		
	}

	public void processDefaults(DataOutputBlobWriter<HTTPRequestSchema> writer, int pathId) {
		pathToRoute[pathId].processDefaults(writer);
	}

	public static FieldExtractionDefinitions fieldExDef(HTTPRouterStageConfig<?, ?, ?, ?> that, int pathId) {
		return that.pathToRoute[pathId];
	}

	@Override
	public byte[] jsonOpenAPIBytes(GraphManager graphManager) {
		
//////////////////
//plan TODO: follow the plan
///////////////////
//1. build JAX-RS project to capture example OpenAPI.
//2. CommandChannel get HTTPResponder instance holding (this is GL???)
//   A. response type
//   B. JSON template
//   C. Link this data into HTTPRouterStageConf77ig
//3. HTTPRouterStageConfig builds JSON
//4. Add Telemetry module to return open API
//////
//////
//Estimates
//////
//1. 2 hours
//2. 4 hours
//3. 16 hours
//4. 2 hours 
//////

		JSONObject<HTTPRouterStageConfig, HTTPRouterStageConfig, JSONObject<HTTPRouterStageConfig, HTTPRouterStageConfig, JSONRenderer<HTTPRouterStageConfig>>> paths = new JSONRenderer<HTTPRouterStageConfig>()
		 .startObject()
		 	.string("openapi".getBytes(),(f,w)->{w.write("3.0.1".getBytes());})		 	
		 	.startObject("paths");
		 	
		//loop over paths and add each one? because the keys are dynamic here, (bad idea but...)
		//TODO: once done capture this pattern as new feature .writeMany ???
		
	//	System.out.println("urlMap\n"+this.urlMap);
		
		//TODO: remove 5 way server and use 4 instead.
		//new Exception("XXX need to read index values").printStackTrace();
		
		
		final Set<String> dupDetect = new HashSet<String>();
		final StringBuilder openApiPath = new StringBuilder();
		this.urlMap.visitPatterns((backing,length,value)-> { //why is length passed in??

			int pathId = (int)value;
			if (pathId>=0) {
				int routeId = getRouteIdForPathId((int)pathId);
				if (routeId>=0) {
					
					FieldExtractionDefinitions ep = extractionParser((int)pathId);
		
//					System.out.println("routeId "+routeId+" "+ep.routeId);
//					
//					if (ep!=null) {
//						System.out.println(Arrays.toString(	ep.paramIndexArrayValidator() ));
//						
//						
//						////TODO: these are the field names, is there a better place to get this??
//						//System.out.println(		ep.getRuntimeParser().toString() );
//						
//						ep.getFieldParamParser().visitPatterns((b,l,v)-> {
//							
//							final int argPos = ((int)value&0xFFFF)-1; 			
//							StructType type = CompositeRouteImpl.extractType(b, l, v);
//							
//							int[] paramIndexArray = ep.paramIndexArray(); //TODO: is this not set yet??
//							int x= argPos<paramIndexArray.length ? paramIndexArray[argPos] : -2;
//							//TODO: need to know the order..
//							
//							System.out.println(new String(b,0,l)+" "+v+"  "+type+" argpos "+x+" from "+argPos);
//						});
//						
//
//					}
					
					
					
				}
			}
			
			//how to get pathId or routeId from the id of the url parser??
			
//			System.out.println("debug C");
//			if (null != ep) {
//				
//				
//				JSONExtractorCompleted ex = this.JSONExtractor(ep.routeId);
//			//	ep.
//				
//				ex.debugSchema();
//				
////				System.out.println(ex.trieParser());
//			}
//			System.out.println("debug D");
			
			
//			System.out.println("["+new String(backing,0,length)+"]");
			
			openApiPath.setLength(0);
			for(int i=0; i<length; i++) {				
				int v = backing[i];
				if (v==TrieParser.TYPE_VALUE_BYTES) {
					//do not skip stopper
					
					//TODO: what is the name of this field?
					openApiPath.append("{}");
				} else {
					if (v==TrieParser.TYPE_VALUE_NUMERIC) {
						//skip type
						i++;
						//TODO: what is the name of this field?
						openApiPath.append("{}");
					} else {
						
						openApiPath.append((char)v);
					}
				}
			}
			
			openApiPath.setLength(openApiPath.length()-1); //remove trailing space
			
			 if (length>2) {
				 //TODO: why is 2 needed to remove / ??
				 //visit patterns ?? need to make readable??
				 
				 //if we have already seen this patter do not add again..
				 String pathString = openApiPath.toString();
				 
				 if (!dupDetect.contains(pathString)) {
					 
					 //if this matches previous skip..
					 
					 paths.startObject(pathString) //remove white space
							//verbs??
							.endObject();
					 
					 dupDetect.add(pathString);
				 }
			 }
			
		});
		
		 	
		 JSONRenderer<HTTPRouterStageConfig> rend = paths.endObject()
		 	.startObject("components")
			 	.startObject("schemas")
			 	//??
			 	.endObject()
		 	.endObject()
		 .endObject();
		
		AppendableBuilder builder = new AppendableBuilder();
		rend.render(builder, this);
		return builder.toBytes();//NOTE: may want to return builder instead?
		
	}

}
