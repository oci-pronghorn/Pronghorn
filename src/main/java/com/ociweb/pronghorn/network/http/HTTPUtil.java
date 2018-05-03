package com.ociweb.pronghorn.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ServerCoordinator;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevision;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructType;
import com.ociweb.pronghorn.util.TrieParser;

public class HTTPUtil {

	private static final Logger logger = LoggerFactory.getLogger(HTTPUtil.class);
    protected static final byte[] ZERO = new byte[] {'0'};
	
    private final static int CHUNK_SIZE = 1;
    private final static int CHUNK_SIZE_WITH_EXTENSION = 2;
    
	static final TrieParser chunkMap = buildChunkMap();
    
	public static void publishStatus(long channelId, int sequence,
	            					int status,
	            					Pipe<ServerResponseSchema> localOutput) {

		int channelIdHigh = (int)(channelId>>32); 
		int channelIdLow = (int)channelId;		
		
		publishStatus(sequence, status, channelIdHigh, channelIdLow, localOutput);
	}

	public static void publishStatus(int sequence, int status, int channelIdHigh, int channelIdLow,
			Pipe<ServerResponseSchema> localOutput) {
		int contentLength = 0;
		byte[] contentBacking = null;
		int contentPosition = 0;
		int contentMask = Integer.MAX_VALUE;
		
		simplePublish(ServerCoordinator.END_RESPONSE_MASK, sequence, status, localOutput, channelIdHigh, channelIdLow, null,
				      contentLength, contentBacking, contentPosition, contentMask);
	}
    
	private static TrieParser buildChunkMap() {
		  TrieParser chunkMap = new TrieParser(128,true);
	      chunkMap.setUTF8Value("%U\r\n", CHUNK_SIZE); //hex parser of U% does not require leading 0x
	      chunkMap.setUTF8Value("%U;%b\r\n", CHUNK_SIZE_WITH_EXTENSION);
	      return chunkMap;
	}

	public static void simplePublish(int requestContext, int sequence, int status,
			Pipe<ServerResponseSchema> localOutput, int channelIdHigh, int channelIdLow, byte[] typeBytes,
			int contentLength, byte[] contentBacking, int contentPosition, int contentMask) {
		assert(contentLength>=0) : "This method does not support chunking";
		
		byte [] contentLocationBacking = null;
		int contentLocationBytesPos = 0;
		int contentLocationBytesLen = 0;
		int contentLocationBytesMask = 0;
		
		int headerSize = Pipe.addMsgIdx(localOutput, ServerResponseSchema.MSG_TOCHANNEL_100); //channel, sequence, context, payload 
	
	    Pipe.addIntValue(channelIdHigh, localOutput);
	    Pipe.addIntValue(channelIdLow, localOutput);
	    Pipe.addIntValue(sequence, localOutput);
	    
	    DataOutputBlobWriter<ServerResponseSchema> writer = Pipe.outputStream(localOutput);        
	    writer.openField();
		boolean chunked = false;
		boolean server = false;
		byte[] eTagBytes = null;
		AbstractRestStage.writeHeader(  HTTPRevisionDefaults.HTTP_1_1.getBytes(),
				                        status, requestContext, eTagBytes, 
				                        typeBytes, 
				                        contentLength, 
						    		    chunked, server,
						    		    contentLocationBacking, contentLocationBytesPos,contentLocationBytesLen,contentLocationBytesMask,
						    		    writer, 1&(requestContext>>ServerCoordinator.CLOSE_CONNECTION_SHIFT));
	    if (contentLength>0) {
			writer.write(contentBacking, contentPosition, contentLength, contentMask);
	    }
		writer.closeLowLevelField();          
	
	    Pipe.addIntValue(requestContext , localOutput); //empty request context, set the full value last.                        
	    
	    Pipe.confirmLowLevelWrite(localOutput, headerSize);
	    Pipe.publishWrites(localOutput);
	    
	    //logger.info("published error {} ",status);
	}


	public static void addHeader(TrieParser headerParser, long value, CharSequence template) {
		
		//logger.info("building parsers for: {} {}",template,((0xFF)&value));
		
		headerParser.setUTF8Value(template, "\r\n", value);
		if (HTTPSpecification.supportWrongLineFeeds) {				
			headerParser.setUTF8Value(template, "\n", value);
		}
	}

	public static void addHeader(StructRegistry schema, int structId, TrieParser headerParser, HTTPHeader header) {
		long fieldId = schema.growStruct(structId,
				StructType.Blob,	//TODO: need a way to define dimensions on headers
				0,
				//NOTE: associated object will be used to interpret 
				header.rootBytes());
		
		if (!schema.setAssociatedObject(fieldId, header)) {
			throw new UnsupportedOperationException("A header with the same identity hash is already held, can not add "+header);
		}
						
		assert(schema.getAssociatedObject(fieldId) == header) : "unable to get associated object";
		assert(fieldId == schema.fieldLookupByIdentity(header, structId));
		
		
		CharSequence template = header.readingTemplate();
		addHeader(headerParser, fieldId, template);
	}

	public static TrieParser buildHeaderParser(StructRegistry schema, int structId, HTTPHeader ... headers) {
				
		boolean skipDeepChecks = false;
		boolean supportsExtraction = true;
		boolean ignoreCase = true;
		TrieParser headerParser = new TrieParser(256,4,skipDeepChecks,supportsExtraction,ignoreCase);
	
		addHeader(headerParser,HTTPSpecification.END_OF_HEADER_ID, "");
						
		int i = headers.length;
		while (--i>=0) {
			addHeader(schema, structId, headerParser, headers[i]);
		}
				
		addHeader(headerParser, HTTPSpecification.UNKNOWN_HEADER_ID, "%b: %b");
		
		return headerParser;
	}

	public static int newHTTPStruct(StructRegistry typeData) {
		//all HTTP structs must start with payload as the first field, this is required
		//so the payload processing stages do not need to look up this common index.
		return typeData.addStruct(new byte[][] {"payload".getBytes()},
				                  new StructType[] {StructType.Blob},
				                  new int[] {0});
	}

}
