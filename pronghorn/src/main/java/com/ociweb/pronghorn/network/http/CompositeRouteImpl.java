package com.ociweb.pronghorn.network.http;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.json.JSONRequired;
import com.ociweb.pronghorn.network.ServerConnectionStruct;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.struct.ByteSequenceValidator;
import com.ociweb.pronghorn.struct.DecimalValidator;
import com.ociweb.pronghorn.struct.LongValidator;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructType;
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
	private final HTTPRouterStageConfig<?,?,?,?> config;
	private final ArrayList<FieldExtractionDefinitions> defs;
	
	public final int structId;
    private final ServerConnectionStruct scs;
	
    private int[] activePathFieldIndexPosLookup;
    private Object[] activePathFieldValidator;

    
    private TrieParserVisitor modifyStructVisitor = new TrieParserVisitor() {
		@Override
		public void visit(byte[] pattern, int length, long value) {
			final int argPos = ((int)value&0xFFFF)-1;
			
			StructType type = null;
			switch((int)(value>>16)) {
				case TrieParser.ESCAPE_CMD_SIGNED_INT:
					type = StructType.Long;
					break;			
				case TrieParser.ESCAPE_CMD_RATIONAL:
					type = StructType.Rational;
					break;
				case TrieParser.ESCAPE_CMD_DECIMAL:
					type = StructType.Decimal;
					break;
				case TrieParser.ESCAPE_CMD_BYTES:
					type = StructType.Blob;
					break;
				default:
					throw new UnsupportedOperationException("unknown value of "+(value>>16)+" for key "+new String(Arrays.copyOfRange(pattern, 0, length)));
			}
						
			final long fieldId = scs.registry.modifyStruct(structId, pattern, 0, length, type, 0);
			
			//must build a list of fieldId ref in the order that these are disovered
			//at postion inURL must store fieldId for use later... where is this held?
			//one per path.
			activePathFieldIndexPosLookup[argPos] = (int)fieldId & StructRegistry.FIELD_MASK;
			activePathFieldValidator[argPos] = scs.registry.fieldValidator(fieldId);
			
		}
    };

    
	public CompositeRouteImpl(ServerConnectionStruct scs,
			                  HTTPRouterStageConfig<?,?,?,?> config,
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
		this.scs = scs;
	    
		this.structId = HTTPUtil.newHTTPStruct(scs.registry);	
		if (null != this.extractor) {
			this.extractor.addToStruct(scs.registry, structId);
		}
		/////////////////////////
		//add the headers to the struct
	    //always add parser in order to ignore headers if non are requested.
		boolean skipDeepChecks = false; //TODO: this may help but removing the wild card support would be better...
		boolean supportsExtraction = true;
		boolean ignoreCase = true;
		TrieParser headerParser = new TrieParser(256,4,skipDeepChecks,supportsExtraction,ignoreCase);

		HTTPUtil.addHeader(headerParser,HTTPSpecification.END_OF_HEADER_ID,"");
		
		//these added for logical req
		boolean headerContentLength = false;
		boolean headerTransferEncodeing = false;
		boolean headerConnection = false;
		
		//these added for performance
		boolean hostHeader = false;
		boolean contentType = false;
		boolean language = false;
		boolean encoding = false;
		boolean referer = false;
		boolean origin = false;
		boolean dnt = false;
		boolean user = false;
		
		if (null!=headers) {			
			int h = headers.length;
			while (--h >= 0) {
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
				if (Arrays.equals(HTTPHeaderDefaults.HOST.rootBytes(),header.rootBytes())) {
					hostHeader = true;
				}
				if (Arrays.equals(HTTPHeaderDefaults.CONTENT_TYPE.rootBytes(),header.rootBytes())) {
					contentType = true;
				}
				if (Arrays.equals(HTTPHeaderDefaults.ACCEPT_LANGUAGE.rootBytes(),header.rootBytes())) {
					language = true;
				}
				if (Arrays.equals(HTTPHeaderDefaults.ACCEPT_ENCODING.rootBytes(),header.rootBytes())) {
					encoding = true;
				}
				if (Arrays.equals(HTTPHeaderDefaults.REFERER.rootBytes(),header.rootBytes())) {
					referer = true;				
				}
				if (Arrays.equals(HTTPHeaderDefaults.ORIGIN.rootBytes(),header.rootBytes())) {
					origin = true;
				}
				if (Arrays.equals(HTTPHeaderDefaults.DNT.rootBytes(),header.rootBytes())) {
					dnt = true;
				}
				if (Arrays.equals(HTTPHeaderDefaults.USER_AGENT.rootBytes(),header.rootBytes())) {
					user = true;
				}
				
				
				HTTPUtil.addHeader(scs.registry, structId, headerParser, header);
			}
			
		}
		
		if (!headerContentLength) {
			HTTPUtil.addHeader(scs.registry, structId, headerParser, HTTPHeaderDefaults.CONTENT_LENGTH);
		}
		if (!headerTransferEncodeing) {
			HTTPUtil.addHeader(scs.registry, structId, headerParser, HTTPHeaderDefaults.TRANSFER_ENCODING);
		}
		if (!headerConnection) {
			HTTPUtil.addHeader(scs.registry, structId, headerParser, HTTPHeaderDefaults.CONNECTION);
		}
		if (!hostHeader) {
			HTTPUtil.addHeader(scs.registry, structId, headerParser, HTTPHeaderDefaults.HOST);
		}
		if (!contentType) {
			HTTPUtil.addHeader(scs.registry, structId, headerParser, HTTPHeaderDefaults.CONTENT_TYPE);
		}
		if (!language) {
			HTTPUtil.addHeader(scs.registry, structId, headerParser, HTTPHeaderDefaults.ACCEPT_LANGUAGE);
		}
		if (!encoding) {
			HTTPUtil.addHeader(scs.registry, structId, headerParser, HTTPHeaderDefaults.ACCEPT_ENCODING);
		}
		if (!referer) {
			HTTPUtil.addHeader(scs.registry, structId, headerParser, HTTPHeaderDefaults.REFERER);
		}
		if (!origin) {
			HTTPUtil.addHeader(scs.registry, structId, headerParser, HTTPHeaderDefaults.ORIGIN);
		}
		if (!dnt) {
			HTTPUtil.addHeader(scs.registry, structId, headerParser, HTTPHeaderDefaults.DNT);
		}
		if (!user) {
			HTTPUtil.addHeader(scs.registry, structId, headerParser, HTTPHeaderDefaults.USER_AGENT);
		}
				
				
		HTTPHeader[] toEcho = scs.headersToEcho();
		if (null != toEcho) {
			int h = toEcho.length;
			while (--h >= 0) {
				HTTPUtil.addHeader(scs.registry, structId, headerParser, toEcho[h]);
			}
		}
		
		HTTPUtil.addHeader(headerParser,HTTPSpecification.UNKNOWN_HEADER_ID,"%b: %b");

		config.storeRouteHeaders(routeId, headerParser);	
		
	}

	@Override
	public int routeId(Object associatedObject) {		
		scs.registry.registerStructAssociation(structId, associatedObject);
		config.registerRouteAssociation(routeId, associatedObject);

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
		activePathFieldValidator = new Object[fieldExDef.getIndexCount()];

		//this visitor will populate the above 2 member arrays we just created 
		fieldExDef.getRuntimeParser().visitPatterns(modifyStructVisitor);
		
		fieldExDef.setPathFieldLookup(activePathFieldIndexPosLookup, activePathFieldValidator);
		
		config.storeRequestExtractionParsers(pathsId, fieldExDef); //this looked up by pathId
		config.storeRequestedJSONMapping(routeId, extractor);
	
		assert(structId == config.getStructIdForRouteId(routeId));
		
		defs.add(fieldExDef);
		
		//System.out.println("added path:"+path);
		//System.out.println("known routes defined:\n"+parser.debugRouterMap());
		return this;
	}
	
	@Override
	public CompositeRouteFinish defaultInteger(String key, long value) {
		byte[] keyBytes = key.getBytes();
		
		//logger.info("\nModify struct {} add key {} ", structId, key);
		scs.registry.modifyStruct(structId, keyBytes, 0, keyBytes.length, StructType.Long, 0);
		
		int i = defs.size();
		assert(i>0);
		while (--i>=0) {
			defs.get(i).defaultInteger(keyBytes, value, scs.registry);			
		}
		return this;
	}

	@Override
	public CompositeRouteFinish defaultText(String key, String value) {
		byte[] keyBytes = key.getBytes();
		scs.registry.modifyStruct(structId, keyBytes, 0, keyBytes.length, StructType.Text, 0);
		
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultText(keyBytes, value, scs.registry);			
		}
		return this;
	}

	@Override
	public CompositeRouteFinish defaultDecimal(String key, long m, byte e) {
		byte[] keyBytes = key.getBytes();
		scs.registry.modifyStruct(structId, keyBytes, 0, keyBytes.length, StructType.Decimal, 0);
		
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultDecimal(keyBytes, m, e, scs.registry);			
		}
		return this;
	}
	
	@Override
	public CompositeRouteFinish defaultRational(String key, long numerator, long denominator) {
		byte[] keyBytes = key.getBytes();
		scs.registry.modifyStruct(structId, keyBytes, 0, keyBytes.length, StructType.Rational, 0);
		
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultRational(keyBytes, numerator, denominator, scs.registry);			
		}
		return this;
	}

	@Override
	public CompositeRouteFinish associatedObject(String key, Object object) {		
		long fieldLookup = scs.registry.fieldLookup(key, structId);

		assert(-1 != fieldLookup) : "Unable to find associated key "+key;
		scs.registry.setAssociatedObject(fieldLookup, object);
		
		assert(fieldLookup == scs.registry.fieldLookupByIdentity(object, structId));
		
		return this;
	}

	@Override
	public CompositeRouteFinish refineInteger(String key, Object associatedObject, long defaultValue) {
		associatedObject(key,associatedObject);
		defaultInteger(key, defaultValue);
		return this;
	}

	@Override
	public CompositeRouteFinish refineText(String key, Object associatedObject, String defaultValue) {
		associatedObject(key,associatedObject);
		defaultText(key, defaultValue);
		return this;
	}

	@Override
	public CompositeRouteFinish refineDecimal(String key, Object associatedObject, long mantissa, byte exponent) {
		associatedObject(key,associatedObject);
		defaultDecimal(key, mantissa, exponent); 
		return this;
	}

	@Override
	public CompositeRouteFinish refineInteger(String key, Object associatedObject, long defaultValue, LongValidator validator) {
		long fieldLookup = scs.registry.fieldLookup(key, structId);
		if (-1 == fieldLookup) {
			throw new UnsupportedOperationException("The field "+key+" was not found defined above");
		}
		scs.registry.setAssociatedObject(fieldLookup, associatedObject);		
		assert(fieldLookup == scs.registry.fieldLookupByIdentity(associatedObject, structId));	
		scs.registry.setValidator(fieldLookup, JSONRequired.REQUIRED, validator);
		
		defaultInteger(key, defaultValue);
		return this;
	}

	@Override
	public CompositeRouteFinish refineText(String key, Object associatedObject, String defaultValue, ByteSequenceValidator validator) {
		long fieldLookup = scs.registry.fieldLookup(key, structId);		
		if (-1 == fieldLookup) {
			throw new UnsupportedOperationException("The field "+key+" was not found defined above");
		}
		scs.registry.setAssociatedObject(fieldLookup, associatedObject);		
		assert(fieldLookup == scs.registry.fieldLookupByIdentity(associatedObject, structId));	
		scs.registry.setValidator(fieldLookup, JSONRequired.REQUIRED, validator);
		defaultText(key, defaultValue);
		return this;
	}

	@Override
	public CompositeRouteFinish refineDecimal(String key, Object associatedObject, long defaultMantissa, byte defaultExponent, DecimalValidator validator) {
		long fieldLookup = scs.registry.fieldLookup(key, structId);		
		if (-1 == fieldLookup) {
			throw new UnsupportedOperationException("The field "+key+" was not found defined above");
		}
		scs.registry.setAssociatedObject(fieldLookup, associatedObject);		
		assert(fieldLookup == scs.registry.fieldLookupByIdentity(associatedObject, structId));	
		scs.registry.setValidator(fieldLookup, JSONRequired.REQUIRED, validator);
		defaultDecimal(key, defaultMantissa, defaultExponent);//NOTE: we must always set the default AFTER the validator
		return this;
	}

	@Override
	public CompositeRouteFinish refineInteger(String key, Object associatedObject, LongValidator validator) {
		long fieldLookup = scs.registry.fieldLookup(key, structId);		
		if (-1 == fieldLookup) {
			throw new UnsupportedOperationException("The field "+key+" was not found defined above");
		}
		scs.registry.setAssociatedObject(fieldLookup, associatedObject);		
		assert(fieldLookup == scs.registry.fieldLookupByIdentity(associatedObject, structId));	
		scs.registry.setValidator(fieldLookup, JSONRequired.REQUIRED, validator);

		return this;
	}

	@Override
	public CompositeRouteFinish refineText(String key, Object associatedObject, ByteSequenceValidator validator) {
		long fieldLookup = scs.registry.fieldLookup(key, structId);		
		if (-1 == fieldLookup) {
			throw new UnsupportedOperationException("The field "+key+" was not found defined above");
		}
		scs.registry.setAssociatedObject(fieldLookup, associatedObject);		
		assert(fieldLookup == scs.registry.fieldLookupByIdentity(associatedObject, structId));	
		scs.registry.setValidator(fieldLookup, JSONRequired.REQUIRED, validator);

		return this;
	}

	@Override
	public CompositeRouteFinish refineDecimal(String key, Object associatedObject, DecimalValidator validator) {
		long fieldLookup = scs.registry.fieldLookup(key, structId);		
		if (-1 == fieldLookup) {
			throw new UnsupportedOperationException("The field "+key+" was not found defined above");
		}
		scs.registry.setAssociatedObject(fieldLookup, associatedObject);		
		assert(fieldLookup == scs.registry.fieldLookupByIdentity(associatedObject, structId));	
		scs.registry.setValidator(fieldLookup, JSONRequired.REQUIRED, validator);

		return this;
	}
	
	@Override
	public CompositeRouteFinish validator(String key, JSONRequired required, LongValidator validator) {
		long fieldLookup = scs.registry.fieldLookup(key, structId);
		if (-1 == fieldLookup) {
			throw new UnsupportedOperationException("The field "+key+" was not found defined above");
		}
		scs.registry.setValidator(fieldLookup, required, validator);		
		return this;
	}

	@Override
	public CompositeRouteFinish validator(String key, JSONRequired required, ByteSequenceValidator validator) {
		long fieldLookup = scs.registry.fieldLookup(key, structId);
		if (-1 == fieldLookup) {
			throw new UnsupportedOperationException("The field "+key+" was not found defined above");
		}
		scs.registry.setValidator(fieldLookup, required, validator);		
		return this;
	}

	@Override
	public CompositeRouteFinish validator(String key, JSONRequired required, DecimalValidator validator) {
		long fieldLookup = scs.registry.fieldLookup(key, structId);
		if (-1 == fieldLookup) {
			throw new UnsupportedOperationException("The field "+key+" was not found defined above");
		}
		scs.registry.setValidator(fieldLookup, required, validator);		
		return this;
	}



}
