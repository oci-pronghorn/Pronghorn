package com.ociweb.pronghorn.network.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.HTTPUtilResponse;
import com.ociweb.pronghorn.network.OrderSupervisorStage;
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
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;

public class HTTPUtil {
	
    public static boolean developerModeCacheDisable = false;

    protected static final byte[] SERVER = "Server: GreenLightning\r\n".getBytes();//Apache/1.3.3.7 (Unix) (Red-Hat/Linux)".getBytes();
    protected static final byte[] ETAG = "ETag: ".getBytes();
    
    protected static final byte[] ZERO = new byte[] {'0'};
    
//  HTTP/1.1 200 OK
//  Server: nginx/1.10.0 (Ubuntu)
//  Date: Mon, 16 Jan 2017 17:18:27 GMT
//  Content-Type: application/json
//  Content-Length: 30
//  Last-Modified: Mon, 16 Jan 2017 16:50:59 GMT
//  Connection: keep-alive
//  ETag: "587cf9f3-1e"
//  Accept-Ranges: bytes
//
//  {"x":9,"y":17,"groovySum":26}
  
 //private static final byte[] EXTRA_STUFF = "Date: Mon, 16 Jan 2017 17:18:27 GMT\r\nLast-Modified: Mon, 16 Jan 2017 16:50:59 GMT\r\nETag: \"587cf9f3-1e\"\r\nAccept-Ranges: bytes\r\n".getBytes();
  
    
    public static final byte[] RETURN_NEWLINE = "\r\n".getBytes();

	static final byte[] EXPIRES_ZERO = "Expires: 0\r\n".getBytes();

	static final byte[] PRAGMA_NO_CACHE = "Pragma: no-cache\r\n".getBytes();

	static final byte[] CACHE_CONTROL_NO_CACHE = "Cache-Control: no-cache, no-store, must-revalidate\r\n".getBytes();

	private static final Logger logger = LoggerFactory.getLogger(HTTPUtil.class);
	
    private final static int CHUNK_SIZE = 1;
    private final static int CHUNK_SIZE_WITH_EXTENSION = 2;
    
    
    static final byte[][] CONNECTION = new byte[][] {
        "Connection: open".getBytes(),
        "Connection: close".getBytes()
    };
    
    protected static final byte[] CONTENT_TYPE = "Content-Type: ".getBytes();
    protected static final byte[] CONTENT_LENGTH = "Content-Length: ".getBytes();
    protected static final byte[] CONTENT_LOCATION = "Content-Location: ".getBytes();
	protected static final byte[] CONTENT_CHUNKED = "Transfer-Encoding: chunked".getBytes();
	
	static final TrieParser chunkMap = buildChunkMap();
	
	
	public static final int COMPRESSOIN_GZIP    = 1;
	public static final int COMPRESSOIN_DEFLATE = 2;
	
	public static final TrieParser compressionEncodings = buildCompressionEncodings();
    
	public static void publishStatus(long channelId, int sequence,
	            					int status,
	            					Pipe<ServerResponseSchema> localOutput) {

		int channelIdHigh = (int)(channelId>>32); 
		int channelIdLow = (int)channelId;		
		
		publishStatus(sequence, status, channelIdHigh, channelIdLow, localOutput);
	}

	private static TrieParser buildCompressionEncodings() {
		
		TrieParser result = new TrieParser();
		
		result.setUTF8Value(",", 0);	
		result.setUTF8Value(" ", 0);	
		result.setUTF8Value("gzip", COMPRESSOIN_GZIP);		
		result.setUTF8Value("deflate", COMPRESSOIN_DEFLATE);
		
		return result;
	}

	public static void publishStatus(int sequence, int status, int channelIdHigh, int channelIdLow,
			Pipe<ServerResponseSchema> localOutput) {
		int contentLength = 0;
		byte[] contentBacking = null;
		int contentPosition = 0;
		int contentMask = Integer.MAX_VALUE;
		
		publishArrayResponse(ServerCoordinator.END_RESPONSE_MASK, sequence, status, localOutput, channelIdHigh, channelIdLow, null,
				      contentLength, contentBacking, contentPosition, contentMask);
	}
    
	private static TrieParser buildChunkMap() {
		  TrieParser chunkMap = new TrieParser(128,true);
	      chunkMap.setUTF8Value("%U\r\n", CHUNK_SIZE); //hex parser of U% does not require leading 0x
	      chunkMap.setUTF8Value("%U;%b\r\n", CHUNK_SIZE_WITH_EXTENSION);
	      return chunkMap;
	}
	

	public static void publishArrayResponse(int requestContext, int sequence, int status,
			Pipe<ServerResponseSchema> localOutput, long channelId, byte[] typeBytes,
			int contentLength, byte[] contentBacking, int contentPosition, int contentMask) {
		
		int channelIdHigh = (int)(channelId>>32); 
		int channelIdLow = (int)channelId;
		
		publishArrayResponse(requestContext, sequence, status, localOutput,
				channelIdHigh, channelIdLow, typeBytes, contentLength, contentBacking, contentPosition, contentMask);
		
	}
	
	public static void publishArrayResponse(int requestContext, int sequence, int status,
			Pipe<ServerResponseSchema> localOutput, int channelIdHigh, int channelIdLow, byte[] typeBytes,
			int contentLength, byte[] contentBacking, int contentPosition, int contentMask) {
		assert(contentLength>=0) : "This method does not support chunking";
		
		int size = Pipe.addMsgIdx(localOutput, ServerResponseSchema.MSG_TOCHANNEL_100); //channel, sequence, context, payload 
		    
	    Pipe.addIntValue(channelIdHigh, localOutput);
	    Pipe.addIntValue(channelIdLow, localOutput);
	    Pipe.addIntValue(sequence, localOutput);
	    
	    DataOutputBlobWriter<ServerResponseSchema> writer = Pipe.openOutputStream(localOutput);        

		boolean chunked = false;
		boolean server = false;
		byte[] eTagBytes = null;
		HTTPUtil.writeHeader(  HTTPRevisionDefaults.HTTP_1_1.getBytes(),
				                        status, requestContext, eTagBytes, 
				                        typeBytes, 
				                        contentLength, 
						    		    chunked, server,
						    		    writer,
						    		    1&(requestContext>>ServerCoordinator.CLOSE_CONNECTION_SHIFT),
						    		    null);
	    if (contentLength>0) {
			writer.write(contentBacking, contentPosition, contentLength, contentMask);
	    }
		writer.closeLowLevelField();          
	
	    Pipe.addIntValue(requestContext , localOutput); //empty request context, set the full value last.                        
	    
	    Pipe.confirmLowLevelWrite(localOutput, size);
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

	public static void writeHeader(byte[] revisionBytes, int status, int requestContext,
			                       byte[] etagBytes, byte[] typeBytes, 
			                       int length, boolean chunked, boolean server,
			                       DataOutputBlobWriter<ServerResponseSchema> writer, 
			                       int conStateIdx, HeaderWritable hw) {
	
	        //line one
		    DataOutputBlobWriter.write(writer, revisionBytes, 0, revisionBytes.length);
	            	    
		    byte[] message = HTTPResponseStatusCodes.codes[status];
		    if (null!=message) {
		    	DataOutputBlobWriter.write(writer, message, 0, message.length);		    	
		    } else {
		    	throw new UnsupportedOperationException("Unknwown status "+status);
		    }
	        
	        //line two
	        if (server) {
	        	writer.write(SERVER);
	        }
	        
	        if (null!=etagBytes) {
	            writer.write(ETAG);
	            writer.write(etagBytes); //ETag: "3f80f-1b6-3e1cb03b"
	            writer.writeByte('\r');
	            writer.writeByte('\n');
	        }          
	        
	        //turns off all client caching, enable for development mode
	        if (developerModeCacheDisable) {
	        	writer.write(CACHE_CONTROL_NO_CACHE);
	        	writer.write(PRAGMA_NO_CACHE);
	        	writer.write(EXPIRES_ZERO);
	        }
	        
	        //line three
	        if (null!=typeBytes) {
	            writer.write(CONTENT_TYPE);
	            writer.write(typeBytes);
	            writer.writeByte('\r');
	            writer.writeByte('\n');
	        }
	
	        //line four
	        if (!chunked) {
	            writer.write(CONTENT_LENGTH);
	            Appendables.appendValue(writer, length);
	            writer.writeByte('\r');
	            writer.writeByte('\n');
	        } else {
	        	writer.write(CONTENT_CHUNKED); 
	            writer.writeByte('\r');
	            writer.writeByte('\n');
	        }
	        
	        //line five            
	        writer.write(CONNECTION[conStateIdx]);
	        writer.writeByte('\r');
	        writer.writeByte('\n');
	        
	        if (null!=hw) {
	        	hw.write(HeaderWriterLocal.get().target(writer));
	        	HeaderWriterLocal.get().target(null);
	        }
	                    
	        writer.writeByte('\r');
	        writer.writeByte('\n');
	        //now ready for content
	        
	        if (chunked) {
	        	//must write the chunk size for the first chunk
	        	//we subtract 2 because the previous chunk is sending RETURN_NEWLINE at its end
	        	final int chunkSize = length-2;
	        			
	        	Appendables.appendHexDigitsRaw(writer, chunkSize);
	        	writer.write(RETURN_NEWLINE);
	                 	
	        }   
	
	}

	public static void prependBodyWithHeader(
			Pipe<ServerResponseSchema> output,
			byte[] etagBytes,
			int totalLengthWritten,
			HTTPUtilResponse ebh,
			int requestContext, long channelId, int sequenceNo,
			byte[] contentType, HeaderWritable additionalHeaderWriter, int status) {
		
		
		final boolean isChunked = false;
		final boolean isServer = true;
		
		//we have not moved any working blob headers yet so this is still the start
		//of the var len stream field
		int startPos = Pipe.getWorkingBlobHeadPosition(output);
		
		//Writes all the data as a series of fragments
		int lengthCount = totalLengthWritten;
		do {
			
			////////////
			int size = Pipe.addMsgIdx(output, ServerResponseSchema.MSG_TOCHANNEL_100);
			Pipe.addLongValue(channelId, output);
			Pipe.addIntValue(sequenceNo, output);
			
			int blockLen = Math.min(lengthCount, output.maxVarLen);
			Pipe.addBytePosAndLenSpecial(output, startPos, blockLen);
			
			startPos += blockLen;
			lengthCount -= blockLen;
						
			Pipe.addIntValue(
					requestContext 
					| (lengthCount==0 ? OrderSupervisorStage.END_RESPONSE_MASK : 0), 
					output);//request context
			
			Pipe.confirmLowLevelWrite(output, size);
	
			//rotate around until length is done...
		} while (lengthCount>0);
		
		
		/////////////
		//reposition to header so we can write it
		///////////
		DataOutputBlobWriter<ServerResponseSchema> localOut = Pipe.outputStream(output);
		HTTPUtilResponse.openToEmptyBlock(ebh, localOut);
		
		writeHeader(
					HTTPRevisionDefaults.HTTP_1_1.getBytes(), //our supported revision
		 		    status, requestContext, 
		 		    etagBytes, contentType, 
		 		    totalLengthWritten, 
		 		    isChunked, isServer,
		 		    localOut, 
		 		    1&(requestContext>>ServerCoordinator.CLOSE_CONNECTION_SHIFT),
		 		    additionalHeaderWriter);
		
		HTTPUtilResponse.finalizeLengthOfFirstBlock(ebh, localOut);
				
		assert(localOut.length()<=output.maxVarLen): "Header is too large or pipe max var size of "+output.maxVarLen+" is too small";
		//logger.trace("built new header response of length "+length);
		
		Pipe.publishWrites(output); //publish both the header and the payload which follows
	}

}
