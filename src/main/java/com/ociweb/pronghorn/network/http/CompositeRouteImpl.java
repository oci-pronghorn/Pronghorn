package com.ociweb.pronghorn.network.http;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructTypes;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.TrieParserReaderLocal;
import com.ociweb.pronghorn.util.TrieParserVisitor;

public class CompositeRouteImpl implements CompositeRoute {

	private static final Logger logger = LoggerFactory.getLogger(CompositeRouteImpl.class);
	
	//TODO: move this entire logic into HTTP1xRouterStageConfig to eliminate this object construction.
	private final JSONExtractorCompleted extractor; 
	private final URLTemplateParser parser; 
	
	private final int routeId;

	private final AtomicInteger pathCounter;
	private final HTTP1xRouterStageConfig<?,?,?,?> config;
	private final ArrayList<FieldExtractionDefinitions> defs;
	
	private final int structId;
    private final StructRegistry schema;
	
    private int[] activePathFieldIndexPosLookup;

    
    private TrieParserVisitor modifyStructVisitor = new TrieParserVisitor() {
		@Override
		public void visit(byte[] pattern, int length, long value) {
			int inURLOrder = (int)value&0xFFFF;
			
			StructTypes type = null;
			switch((int)(value>>16)) {
				case TrieParser.ESCAPE_CMD_SIGNED_INT:
					type = StructTypes.Long;
					break;			
				case TrieParser.ESCAPE_CMD_RATIONAL:
					type = StructTypes.Rational;
					break;
				case TrieParser.ESCAPE_CMD_DECIMAL:
					type = StructTypes.Decimal;
					break;
				case TrieParser.ESCAPE_CMD_BYTES:
					type = StructTypes.Blob;
					break;
				default:
					throw new UnsupportedOperationException("unknown value of "+(value>>16)+" for key "+new String(Arrays.copyOfRange(pattern, 0, length)));
			}
						
			long fieldId = schema.modifyStruct(structId, pattern, 0, length, type, 0);
	
			//must build a list of fieldId ref in the order that these are disovered
			//at postion inURL must store fieldId for use later... where is this held?
			//one per path.
			activePathFieldIndexPosLookup[inURLOrder-1] = (int)fieldId & StructRegistry.FIELD_MASK;
			
		}
    };

    
	public CompositeRouteImpl(StructRegistry schema,
			                  HTTP1xRouterStageConfig<?,?,?,?> config,
			                  JSONExtractorCompleted extractor, 
			                  URLTemplateParser parser, 
			                 
			                  HTTPHeader[] headers,
			                  int routeId,
			                  AtomicInteger pathCounter) {
		
		this.defs = new ArrayList<FieldExtractionDefinitions>();
		this.config = config;
		this.extractor = extractor;
		this.parser = parser;

		//this limits routes to 1 billion.
		assert((routeId & StructRegistry.IS_STRUCT_BIT) == 0) : "routeId must never be confused with StructId";
		
		this.routeId = routeId;
		this.pathCounter = pathCounter;
		this.schema = schema;
	    
		this.structId = HTTPUtil.newHTTPStruct(schema);	
		if (null != this.extractor) {
			this.extractor.addToStruct(schema, structId);
		}
		/////////////////////////
		//add the headers to the struct
	    //always add parser in order to ignore headers if non are requested.
		boolean skipDeepChecks = false;
		boolean supportsExtraction = true;
		boolean ignoreCase = true;
		TrieParser headerParser = new TrieParser(256,4,skipDeepChecks,supportsExtraction,ignoreCase);

		HTTPUtil.addHeader(headerParser,HTTPSpecification.END_OF_HEADER_ID,"");
		
		boolean headerContentLength = false;
		boolean headerTransferEncodeing = false;
		boolean headerConnection = false;
		
		if (null!=headers) {			
			int h = headers.length;
			while (--h>=0) {
				HTTPHeader header = headers[h];
				
				//if not already asked for (this is the server)
				//we must add the required headers...
				//HTTPHeaderDefaults.CONTENT_LENGTH
				//HTTPHeaderDefaults.TRANSFER_ENCODING
				//HTTPHeaderDefaults.CONNECTION 
				if (Arrays.equals(HTTPHeaderDefaults.CONTENT_LENGTH.rootBytes(),header.rootBytes())) {
					headerContentLength = true;
				}
				if (Arrays.equals(HTTPHeaderDefaults.TRANSFER_ENCODING.rootBytes(),header.rootBytes())) {
					headerTransferEncodeing = true;
				}
				if (Arrays.equals(HTTPHeaderDefaults.CONNECTION.rootBytes(),header.rootBytes())) {
					headerConnection = true;
				}
				
				HTTPUtil.addHeader(schema, structId, headerParser, header);
			}
			
		}
		
		if (!headerContentLength) {
			HTTPUtil.addHeader(schema, structId, headerParser, HTTPHeaderDefaults.CONTENT_LENGTH);
		}
		if (!headerTransferEncodeing) {
			HTTPUtil.addHeader(schema, structId, headerParser, HTTPHeaderDefaults.TRANSFER_ENCODING);
		}
		if (!headerConnection) {
			HTTPUtil.addHeader(schema, structId, headerParser, HTTPHeaderDefaults.CONNECTION);
		}
		HTTPUtil.addHeader(headerParser,HTTPSpecification.UNKNOWN_HEADER_ID,"%b: %b");

		config.storeRouteHeaders(routeId, headerParser);	
		
	}

	@Override
	public int routeId(boolean debug) {
		
		if (debug) {
			parser.debugRouterMap("debugRoute");
			
			int i = defs.size();
			while (--i>=0) {
				try {
					defs.get(i).getRuntimeParser().toDOTFile(File.createTempFile("defs"+i,".dot"));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}			
			}
			
		}
		
		return routeId;
	}

	@Override
	public int routeId() {
		return routeId;
	}
	
	@Override
	public CompositeRoute path(CharSequence path) {
		
		int pathsId = pathCounter.getAndIncrement();
		
		//logger.trace("pathId: {} assinged for path: {}",pathsId, path);
		FieldExtractionDefinitions fieldExDef = parser.addPath(path, routeId, pathsId, structId);//hold for defaults..
				
		activePathFieldIndexPosLookup = new int[fieldExDef.getIndexCount()];		
		fieldExDef.getRuntimeParser().visitPatterns(modifyStructVisitor);
		
		fieldExDef.setPathFieldLookup(activePathFieldIndexPosLookup);
		
		config.storeRequestExtractionParsers(pathsId, fieldExDef); //this looked up by pathId
		config.storeRequestedJSONMapping(pathsId, extractor);
	
		assert(structId == config.getStructIdForRouteId(routeId));
		
		defs.add(fieldExDef);
		
		
		return this;
	}
	
	@Override
	public CompositeRouteFinish defaultInteger(String key, long value) {
		byte[] keyBytes = key.getBytes();
		schema.modifyStruct(structId, keyBytes, 0, keyBytes.length, StructTypes.Long, 0);
		
		TrieParserReader reader = TrieParserReaderLocal.get();
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultInteger(reader, keyBytes, value);			
		}
		return this;
	}

	@Override
	public CompositeRouteFinish defaultText(String key, String value) {
		byte[] keyBytes = key.getBytes();
		schema.modifyStruct(structId, keyBytes, 0, keyBytes.length, StructTypes.Text, 0);
		
		TrieParserReader reader = TrieParserReaderLocal.get();
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultText(reader, keyBytes, value);			
		}
		return this;
	}

	@Override
	public CompositeRouteFinish defaultDecimal(String key, long m, byte e) {
		byte[] keyBytes = key.getBytes();
		schema.modifyStruct(structId, keyBytes, 0, keyBytes.length, StructTypes.Decimal, 0);
		
		TrieParserReader reader = TrieParserReaderLocal.get();
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultDecimal(reader, keyBytes, m, e);			
		}
		return this;
	}
	
	@Override
	public CompositeRouteFinish defaultRational(String key, long numerator, long denominator) {
		byte[] keyBytes = key.getBytes();
		schema.modifyStruct(structId, keyBytes, 0, keyBytes.length, StructTypes.Rational, 0);
		
		TrieParserReader reader = TrieParserReaderLocal.get();
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultRational(reader, keyBytes, numerator, denominator);			
		}
		return this;
	}

	@Override
	public CompositeRouteFinish associatedObject(String key, Object object) {		
		schema.setAssociatedObject(schema.fieldLookup(key, structId), object);
		return this;
	}


}
