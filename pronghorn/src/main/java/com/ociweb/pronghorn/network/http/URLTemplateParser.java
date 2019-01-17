package com.ociweb.pronghorn.network.http;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.util.EncodingConverter;
import com.ociweb.pronghorn.util.EncodingConverter.EncodingStorage;
import com.ociweb.pronghorn.util.EncodingConverter.EncodingTransform;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class URLTemplateParser {

	private final static Logger logger = LoggerFactory.getLogger(URLTemplateParser.class);
	private final static byte CUSTOM_ESCAPE = (byte)'"'; //the " char is never allowed to appear in a URL so we can use it here.
	
	private final TrieParser templateParser = buildRouteTemplateParser(new TrieParser(256,1,false,true,false,CUSTOM_ESCAPE));
    private final EncodingConverter converter;
    
    private final TrieParser routerMap;
    private final boolean trustText;
        
    
    public URLTemplateParser(TrieParser routerMap, boolean trustText) {
    	
    	this.converter = new EncodingConverter();
    	this.routerMap = routerMap;
    	this.trustText = trustText;
    	
    }
    
    public void debugRouterMap(String name) {
    	
    	try {
			routerMap.toDOTFile(File.createTempFile(name,".dot"));
		} catch (IOException e) {
			
			
		}
    }
    
    public String debugRouterMap() {
    	return routerMap.toString();
    }
	
	public static TrieParser buildRouteTemplateParser(TrieParser parser) {
				
		assert(parser.ESCAPE_BYTE == CUSTOM_ESCAPE);
		
		parser.setUTF8Value("#{\"b}", TrieParser.ESCAPE_CMD_SIGNED_INT); //%i
		parser.setUTF8Value("#\"b/", TrieParser.ESCAPE_CMD_SIGNED_INT);  //%i
		parser.setUTF8Value("#\"b?", TrieParser.ESCAPE_CMD_SIGNED_INT);  //%i
		parser.setUTF8Value("#\"b&", TrieParser.ESCAPE_CMD_SIGNED_INT);  //%i
		parser.setUTF8Value("#\"b", TrieParser.ESCAPE_CMD_SIGNED_INT);   //%i
		
		parser.setUTF8Value("^{\"b}", TrieParser.ESCAPE_CMD_DECIMAL);  //%i%.
		parser.setUTF8Value("^\"b/", TrieParser.ESCAPE_CMD_DECIMAL);   //%i%.
		parser.setUTF8Value("^\"b?", TrieParser.ESCAPE_CMD_DECIMAL);   //%i%.
		parser.setUTF8Value("^\"b&", TrieParser.ESCAPE_CMD_DECIMAL);   //%i%.
		parser.setUTF8Value("^\"b", TrieParser.ESCAPE_CMD_DECIMAL);    //%i%.
		
		parser.setUTF8Value("${\"b}", TrieParser.ESCAPE_CMD_BYTES);
		parser.setUTF8Value("$\"b?", TrieParser.ESCAPE_CMD_BYTES);
		parser.setUTF8Value("$\"b", TrieParser.ESCAPE_CMD_BYTES);
		parser.setUTF8Value("$\"b&", TrieParser.ESCAPE_CMD_BYTES);
		parser.setUTF8Value("$\"b/", TrieParser.ESCAPE_CMD_BYTES);
		
		parser.setUTF8Value("%{\"b}", TrieParser.ESCAPE_CMD_RATIONAL); //%i%/
		parser.setUTF8Value("%\"b/", TrieParser.ESCAPE_CMD_RATIONAL);  //%i%/
		parser.setUTF8Value("%\"b?", TrieParser.ESCAPE_CMD_RATIONAL);  //%i%?
		parser.setUTF8Value("%\"b&", TrieParser.ESCAPE_CMD_RATIONAL);  //%i%&
		parser.setUTF8Value("%\"b", TrieParser.ESCAPE_CMD_RATIONAL);   //%i%/

		//parser.toDOT(System.out);
		
		return parser;
	}

	//state needed in addRoute due to no lambdas here
	private FieldExtractionDefinitions activeRouteDef;
	private long activePathId;
	
	private final EncodingTransform et = new EncodingTransform() {

		@Override
		public void transform(TrieParserReader templateParserReader,
				DataOutputBlobWriter<RawDataSchema> outputStream) {

			activeRouteDef.setIndexCount(
					convertEncoding(activeRouteDef.getFieldParamParser(), 
							        templateParserReader, 
							        templateParser, 
							        outputStream));
		
		}			
	};
	
	public static boolean showAllInserts = false;//Great to debug the actual routes defined
	
	private final EncodingStorage es = new EncodingStorage() {

		@Override
		public void store(Pipe<RawDataSchema> pipe) {
			
			if (showAllInserts) {
				System.out.println(" map.setUTF8Value(\""+pipe.peekInputStream(pipe, 0).readUTFFully()+"\","+activePathId+");");
			}
			
			//set full byte field in pipe to map with the key routeValue
			//this is the converted to tri parser format text value
			routerMap.setValue(pipe, activePathId);
		}
		
	};
	
	/**
	 * Parse template format and inject TrieParser key into the routerMap.
	 * eg. converts from 
	 * @param path
	 */
	public FieldExtractionDefinitions addPath(CharSequence path,
			                                  int routeId, int pathId, int structId) {

		////////////////////////////////////
		//convert public supported route format eg ${} and #{} into the 
		//internal trie parser format, field names are extracted and added to lookup parser
		////////////////////////////////////		
		activePathId = pathId;
		activeRouteDef = new FieldExtractionDefinitions(trustText, routeId, pathId, structId);		
		converter.convert(path, et, es);
				
		return activeRouteDef;
	}

	private static int convertEncoding(TrieParser runtimeParser, 
			                           TrieParserReader templateParserReader, 
			                           TrieParser templateParser, 
			                           DataOutputBlobWriter<RawDataSchema> outputStream) {
		
		//if we have nothing then use the root /
		if (!TrieParserReader.parseHasContent(templateParserReader)) {
			logger.info("the leading / was added on URL since route did not define it");
			outputStream.writeByte('/');
			return 0;
		}
		
		int fieldIndex = 1; //fields must start with 1
		int lastValue = 0;
		while(TrieParserReader.parseHasContent(templateParserReader)) {
			long token = TrieParserReader.parseNext(templateParserReader, templateParser);
			assert(token<=255) : "type must fit into 8 bits";
			assert(token>=-1);
			
			//32 bit value  [8-flags, 8-type, 16-field index]
			switch ((short)token) {
				case TrieParser.ESCAPE_CMD_RATIONAL:
					
					outputStream.append("%i%/");					
				    TrieParserReader.capturedFieldSetValue(templateParserReader, 0, runtimeParser,
				    		          (token<<16) | (fieldIndex++));//type high 16, and position in low 16
				    fieldIndex++;//takes up 2 spots so we must add one more
					break;
				case TrieParser.ESCAPE_CMD_DECIMAL:
					
					outputStream.append("%i%.");					
				    TrieParserReader.capturedFieldSetValue(templateParserReader, 0, runtimeParser,
				    		          (token<<16) | (fieldIndex++));//type high 16, and position in low 16
				    fieldIndex++;//takes up 2 spots so we must add one more					
					break;
				case TrieParser.ESCAPE_CMD_SIGNED_INT:
					
					outputStream.append("%i");					
				    TrieParserReader.capturedFieldSetValue(templateParserReader, 0, runtimeParser,
				    		          (token<<16) | (fieldIndex++));//type high 16, and position in low 16
										
					break;
				case TrieParser.ESCAPE_CMD_BYTES:
					
					outputStream.append("%b");					
				    TrieParserReader.capturedFieldSetValue(templateParserReader, 0, runtimeParser,
				    		          (token<<16) | (fieldIndex++));//type high 16, and position in low 16
					
					break;
				case -1:
					
					int value = TrieParserReader.parseSkipOne(templateParserReader);
					if (value>=0) {
						if (('/'!=(char)value) && (0 == outputStream.position())) {
							//the leading / was missing so we add it now
							logger.info("the leading / was added on URL since route did not define it");
							outputStream.writeByte('/');
						}
						outputStream.writeByte(value);
						lastValue = value;
					}
					
					break;
			}			
		}
		if (lastValue!=' ') {
			//ensure we always end with ' ' space because in the protocol we find a space
			//after the path, this lets us match exactly what what requested.
			outputStream.writeByte(' ');
		}
		
		//inspect the converted value
		if (logger.isDebugEnabled()) {
			outputStream.debugAsUTF8();
		}
		
		return fieldIndex-1;
	}


}
