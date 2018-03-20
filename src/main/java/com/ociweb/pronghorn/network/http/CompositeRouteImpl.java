package com.ociweb.pronghorn.network.http;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONExtractor;
import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.struct.BStructSchema;
import com.ociweb.pronghorn.struct.BStructTypes;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.TrieParserVisitor;

public class CompositeRouteImpl implements CompositeRoute {

	private static final Logger logger = LoggerFactory.getLogger(CompositeRouteImpl.class);
	
	//TODO: move this entire logic into HTTP1xRouterStageConfig to eliminate this object construction.
	private final JSONExtractorCompleted extractor; 
	private final URLTemplateParser parser; 
	private final IntHashTable headerTable;
	private final int groupId;
	private final AtomicInteger pathCounter;
	private final HTTP1xRouterStageConfig<?,?,?,?> config;
	private final ArrayList<FieldExtractionDefinitions> defs;
	private final TrieParserReader reader = new TrieParserReader(4,true);
	private final int structId;
    private final BStructSchema schema;	
	
    
    private TrieParserVisitor modifyStructVisitor = new TrieParserVisitor() {
		@Override
		public void visit(byte[] pattern, int length, long value) {
			
			BStructTypes type = null;
			switch((int)(value>>16)) {
				case TrieParser.ESCAPE_CMD_SIGNED_INT:
					type = BStructTypes.Long;
					break;			
				case TrieParser.ESCAPE_CMD_RATIONAL:
					type = BStructTypes.Rational;
					break;
				case TrieParser.ESCAPE_CMD_DECIMAL:
					type = BStructTypes.Decimal;
					break;
				case TrieParser.ESCAPE_CMD_BYTES:
					type = BStructTypes.Blob;
					break;
				default:
					throw new UnsupportedOperationException("unknown value of "+(value>>16)+" for key "+new String(Arrays.copyOfRange(pattern, 0, length)));
			}
			
			schema.modifyStruct(structId, pattern, 0, length, type, 0);
		}
    };
    
	public CompositeRouteImpl(BStructSchema schema,
			                  HTTP1xRouterStageConfig<?,?,?,?> config,
			                  JSONExtractorCompleted extractor, 
			                  URLTemplateParser parser, 
			                  IntHashTable headerTable,
			                  HTTPHeader[] headers,
			                  int groupId,
			                  AtomicInteger pathCounter) {
		
		this.defs = new ArrayList<FieldExtractionDefinitions>();
		this.config = config;
		this.extractor = extractor;
		this.parser = parser;
		this.headerTable = headerTable;
		this.groupId = groupId;
		this.pathCounter = pathCounter;
		this.schema = schema;
		
		//begin building the structure with the JSON fields
		if (null==extractor) {
			//create structure with a single payload field
			
			byte[][] fieldNames = new byte[][]{"payload".getBytes()};
			BStructTypes[] fieldTypes = new BStructTypes[]{BStructTypes.Text};//TODO: should be array of bytes..
			int[] fieldDims = new int[]{0};
			this.structId = schema.addStruct(fieldNames, fieldTypes, fieldDims);
		} else {
			this.structId = ((JSONExtractor)extractor).toStruct(schema);
		}
		
		/////////////////////////
		//add the headers to the struct
		if (null!=headers) {
			int h = headers.length;
			while (--h>=0) {
				HTTPHeader header = headers[h];
				
				schema.growStruct(this.structId,
						header.rootBytes(), 
						BStructTypes.Text, //TODO: need custom type per header; 
						0); //TODO: need a way to define dimensions on headers
				
			}
		}
		
		
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
		
		return groupId;
	}

	@Override
	public int routeId() {
		return groupId;
	}
	
	@Override
	public CompositeRoute path(CharSequence path) {
		
		int pathsId = pathCounter.getAndIncrement();
		
		//logger.trace("pathId: {} assinged for path: {}",pathsId, path);
		FieldExtractionDefinitions fieldExDef = parser.addPath(path, groupId, pathsId);//hold for defaults..
		fieldExDef.getRuntimeParser().visitPatterns(modifyStructVisitor);
		
		config.storeRequestExtractionParsers(pathsId, fieldExDef); //this looked up by pathId
		config.storeRequestedJSONMapping(pathsId, extractor);
		config.storeRequestedHeaders(pathsId, headerTable);		
		defs.add(fieldExDef);
		
		
		
		return this;
	}
	
	@Override
	public CompositeRouteFinish defaultInteger(String key, long value) {
		byte[] keyBytes = key.getBytes();
		schema.modifyStruct(structId, keyBytes, 0, keyBytes.length, BStructTypes.Long, 0);
		
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultInteger(reader, keyBytes, value);			
		}
		return this;
	}

	@Override
	public CompositeRouteFinish defaultText(String key, String value) {
		byte[] keyBytes = key.getBytes();
		schema.modifyStruct(structId, keyBytes, 0, keyBytes.length, BStructTypes.Text, 0);
		
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultText(reader, keyBytes, value);			
		}
		return this;
	}

	@Override
	public CompositeRouteFinish defaultDecimal(String key, long m, byte e) {
		byte[] keyBytes = key.getBytes();
		schema.modifyStruct(structId, keyBytes, 0, keyBytes.length, BStructTypes.Decimal, 0);
		
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultDecimal(reader, keyBytes, m, e);			
		}
		return this;
	}
	
	@Override
	public CompositeRouteFinish defaultRational(String key, long numerator, long denominator) {
		byte[] keyBytes = key.getBytes();
		schema.modifyStruct(structId, keyBytes, 0, keyBytes.length, BStructTypes.Rational, 0);
		
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultRational(reader, keyBytes, numerator, denominator);			
		}
		return this;
	}


}
