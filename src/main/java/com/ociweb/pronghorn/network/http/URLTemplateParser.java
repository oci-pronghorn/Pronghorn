package com.ociweb.pronghorn.network.http;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.util.EncodingConverter;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class URLTemplateParser {

	private final TrieParser templateParser = buildRouteTemplateParser(new TrieParser(256,false));
	
    private final EncodingConverter converter;
    
    
    public URLTemplateParser() {
    	
    	converter = new EncodingConverter();

    }
	
	public static TrieParser buildRouteTemplateParser(TrieParser parser) {
				
		parser.setUTF8Value("%%{%b}", TrieParser.ESCAPE_CMD_RATIONAL); //%i%/
		parser.setUTF8Value("%%%b/", TrieParser.ESCAPE_CMD_RATIONAL);  //%i%/
		parser.setUTF8Value("%%%b", TrieParser.ESCAPE_CMD_RATIONAL);   //%i%/
				
		parser.setUTF8Value("^{%b}", TrieParser.ESCAPE_CMD_DECIMAL);  //%i%.
		parser.setUTF8Value("^%b/", TrieParser.ESCAPE_CMD_DECIMAL);   //%i%.
		parser.setUTF8Value("^%b", TrieParser.ESCAPE_CMD_DECIMAL);    //%i%.
				
		parser.setUTF8Value("#{%b}", TrieParser.ESCAPE_CMD_SIGNED_INT); //%i
		parser.setUTF8Value("#%b/", TrieParser.ESCAPE_CMD_SIGNED_INT);  //%i
		parser.setUTF8Value("#%b", TrieParser.ESCAPE_CMD_SIGNED_INT);   //%i
		
		parser.setUTF8Value("${%b}", TrieParser.ESCAPE_CMD_BYTES);
		parser.setUTF8Value("$%b/", TrieParser.ESCAPE_CMD_BYTES);
		parser.setUTF8Value("$%b", TrieParser.ESCAPE_CMD_BYTES);
		
		//for every non match just consume the char and move to the next		
		
		return parser;
	}

	
	/**
	 * Parse template format and inject TrieParser key into the routerMap.
	 * eg. converts from 
	 * @param route
	 * @param routerMap
	 */
	public RouteDef addRoute(CharSequence route, final long routeValue, TrieParser routerMap, boolean trustText) {

		final RouteDef routeDef = new RouteDef(trustText);
		
		converter.convert(route,  
				        (reader, stream) -> {routeDef.setIndexCount(convertEncoding(routeDef.getRuntimeParser(), reader, templateParser, stream));},
				        (pipe) -> {routerMap.setValue(pipe,routeValue);} );
		
		return routeDef;
	}

	
	private static int convertEncoding(TrieParser runtimeParser, TrieParserReader templateParserReader, TrieParser templateParser, DataOutputBlobWriter<RawDataSchema> outputStream) {
		
		int fieldIndex = 1; //fields must start with 1
		int lastValue = 0;
		while(TrieParserReader.parseHasContent(templateParserReader)) {
			long token = TrieParserReader.parseNext(templateParserReader, templateParser);
		
			switch ((int)token) {
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
						outputStream.writeByte(value);
						lastValue = value;
					}
					
					break;
			}			
		}
		if (lastValue!=' ') {
			outputStream.writeByte(' '); //ensure we always end with ' ' space
		}
		return fieldIndex-1;
	}


}
