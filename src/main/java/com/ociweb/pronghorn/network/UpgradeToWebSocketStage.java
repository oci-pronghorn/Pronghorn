package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.http.AbstractRestStage;
import com.ociweb.pronghorn.network.http.HTTPUtil;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

public class UpgradeToWebSocketStage extends PronghornStage {

	private static final byte[] WS_WEBSOCKET = "websocket".getBytes();
	private static final byte[] WS_UPGRADE = "Upgrade".getBytes();
	private static final byte[] WS_VERSION_SUPPORTED = "13".getBytes();
	private final Pipe<HTTPRequestSchema>[] inputPipes;
	private final Pipe<ServerResponseSchema>[] outputs;
	private final HTTPSpecification<?,?,?,?> httpSpec;
	
	private static final int ID_UPGRADE = 1;
	private static final int ID_CONNECTION = 2;
	private static final int ID_SEC_WEBSOCKET_KEY = 21;
	private static final int ID_SEC_WEBSOCKET_PROTOCOL = 22;
	private static final int ID_SEC_WEBSOCKET_VERSION = 23;
	private static final int ID_SEC_WEBSOCKET_EXTENSIONS = 24;
	private static final int ID_ORIGIN = 25;
	
	private static final Logger logger = LoggerFactory.getLogger(UpgradeToWebSocketStage.class);
		
	
	public UpgradeToWebSocketStage(GraphManager graphManager,
			Pipe<HTTPRequestSchema>[] inputPipes,
			Pipe<ServerResponseSchema>[] outputs,
			HTTPSpecification<?,?,?,?> httpSpec) {
		super(graphManager,inputPipes, outputs);
		this.inputPipes = inputPipes;
		this.outputs = outputs;
		this.httpSpec = httpSpec;
		
		if (inputPipes.length>1) {
			GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
		}
		
		assert(httpSpec.headerMatches(ID_UPGRADE,  HTTPHeaderDefaults.UPGRADE.writingRoot()));
		assert(httpSpec.headerMatches(ID_CONNECTION,  HTTPHeaderDefaults.CONNECTION.writingRoot()));
		assert(httpSpec.headerMatches(ID_SEC_WEBSOCKET_KEY, HTTPHeaderDefaults.SEC_WEBSOCKET_KEY.writingRoot()));
		assert(httpSpec.headerMatches(ID_SEC_WEBSOCKET_PROTOCOL, HTTPHeaderDefaults.SEC_WEBSOCKET_PROTOCOL.writingRoot()));
		assert(httpSpec.headerMatches(ID_SEC_WEBSOCKET_VERSION, HTTPHeaderDefaults.SEC_WEBSOCKET_VERSION.writingRoot()));
		//assert(httpSpec.headerMatches(ID_SEC_WEBSOCKET_EXTENSIONS, HTTPHeaderDefaults.SEC_WEBSOCKET_EXTENSIONS.writingRoot()));
		//assert(httpSpec.headerMatches(ID_ORIGIN, HTTPHeaderDefaults.ORIGIN.writingRoot()));
        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
	}

	@Override
	public void run() {
		int i = inputPipes.length;
		while(--i>=0) {
			process(inputPipes[i], outputs[i]);
		}
			
	}

//  int fin = finOpp&1;
//  int opcode = (finOpp>>4);
////*  %x0 denotes a continuation frame //need to arey forward text or binary????
////*  %x1 denotes a text frame
////*  %x2 denotes a binary frame
////*  %x3-7 are reserved for further non-control frames
////*  %x8 denotes a connection close
////*  %x9 denotes a ping //send along??
////*  %xA denotes a pong //send in response to ping with same payload from the ping...
////*  %xB-F are reserved for further control frames
	
	private void process(Pipe<HTTPRequestSchema> input, 
			             Pipe<ServerResponseSchema> output) {

		//TODO: this stage is going to get websocket inputs after upgrade
		//TODO; this websocket upgrade logic should be in a common utility class.
		//      each websocket frame comes in but may interleave with others...
		//TODO: we only want large blocks for index on header, no others.
		//TODO: need a simple stage which talks to MQTT from here??
		

		while (Pipe.hasContentToRead(input)) {
			
		    int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		    	case HTTPRequestSchema.MSG_WEBSOCKETFRAME_100:
		    	    //Read the frame and do something with it
		    		
		    		//if operation is ping must reply
		    	    //public static final int MSG_WEBSOCKETFRAME_100_FIELD_CHANNELID_21 = 0x00800001; //LongUnsigned/Delta/0
		    	    //public static final int MSG_WEBSOCKETFRAME_100_FIELD_SEQUENCE_26 = 0x00400003; //IntegerSigned/None/0
		    	    //public static final int MSG_WEBSOCKETFRAME_100_FIELD_FINOPP_11 = 0x00400004; //IntegerSigned/None/4
		    	    //public static final int MSG_WEBSOCKETFRAME_100_FIELD_MASK_10 = 0x00400005; //IntegerSigned/None/5
		    	    //public static final int MSG_WEBSOCKETFRAME_100_FIELD_BINARYPAYLOAD_12 = 0x01c00006; //ByteVector/None/1

		    		long channelId = Pipe.takeLong(input);
		    		int sequenceId = Pipe.takeInt(input);
		    		int finOppId = Pipe.takeInt(input);
		    		int wsMask = Pipe.takeInt(input);
		    		DataInputBlobReader<HTTPRequestSchema> inputStream = Pipe.openInputStream(input);
		    		
		    		
		    		
		    		
		    		
		    		
		    		break;
		    
		        case HTTPRequestSchema.MSG_RESTREQUEST_300:

///////////////////////////////////////////////////////////////////////	
//		            GET /chat HTTP/1.1
//		            Host: server.example.com
//		            Upgrade: websocket
//		            Connection: Upgrade
//		            Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
//		            Origin: http://example.com
//		            Sec-WebSocket-Protocol: chat, superchat
//		            Sec-WebSocket-Version: 13
///////////////////////////////////////////////////////////////////////		        	
//		respond with             HTTPHeaderDefaults.SEC_WEBSOCKET_ACCEPT.rootBytes(),
		            
		        	//* in GL this route is defined as the upgrade route for this web socket
		        	//* we will need to give it an id so the behaviors can see it as a serial device?
		        	
		        	System.err.println("web socket upgrade request XXXXXXXXXXXXXXXXXXXXXXxxx");
		        	
					long fieldChannelId = Pipe.takeLong(input);
					int fieldSequence = Pipe.takeInt(input);
					
					int routeVerb = Pipe.takeInt(input);
	    	    	int routeId = routeVerb>>>HTTPVerb.BITS;
	    	    	int verbId = HTTPVerb.MASK & routeVerb;
							
					DataInputBlobReader<HTTPRequestSchema> data = Pipe.openInputStream(input);
				
					
					byte[] acceptBacking = null;
					int acceptPosition = 0;
					int acceptLength = -1;
					int acceptMask = 0;
					
					byte[] protocolBacking = null;
					int    protocolPosition = 0;
					int    protocolLength = -1; //signal for none
					int    protocolMask = 0;
					
					
					byte[] expectedOrigin = new byte[0]; //TODO: WHAT IS THE EXPECTED ORIGIN??
					
					boolean isExpectedOrigin = true;//this is an optional field
					
					
					boolean isUpgradeWebsocket = false;
					boolean isConnectionUpgrade = false;
					boolean isValidVersion = true;//this is an optional field
					int id = data.readShort();
					while (id > 0) {
						switch(id) {
							case ID_UPGRADE: //required
								isUpgradeWebsocket = data.equalUTF(WS_WEBSOCKET);
								if (!isUpgradeWebsocket) {
									data.skipBytes(data.readShort());
								}
								break;
							case ID_CONNECTION: //required
								isConnectionUpgrade = data.equalUTF(WS_UPGRADE);
								if (!isConnectionUpgrade) {
									data.skipBytes(data.readShort());
								}
								break;
							case ID_SEC_WEBSOCKET_KEY: //optional used here for signature
								
								//what to do with the key? append etc and hash...
								
								//  If the response lacks a |Sec-WebSocket-Accept| header field or
							    //     the |Sec-WebSocket-Accept| contains a value other than the
							    //   base64-encoded SHA-1 of the concatenation of the |Sec-WebSocket-
							    //   Key| (as a string, not base64-decoded) with the string "258EAFA5-
							    //   E914-47DA-95CA-C5AB0DC85B11" but ignoring any leading and
							    //   trailing whitespace, the client MUST _Fail the WebSocket
							    //   Connection_.
								
								
								//data.readBase64(target arrays)
								
								//1. un base64 encode this field
								//2. append "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
								//3. sha-1 
								//4. base64 encode 
								//   bytes to encode for step 4
								//   Appendables.appendBase64Encoded(target, backing, pos, len, mask)
								
								//5. return as accept
								
								
								data.skipBytes(data.readShort());
								//data.readUTF(target);
								break;
							case ID_SEC_WEBSOCKET_PROTOCOL: //optional for the feed
								
								int length = data.readShort();
								
								//TODO: if we need to filter protocols add a method here.
								
								protocolBacking = input.blobRing;
								protocolMask = input.blobMask;
								protocolPosition = data.absolutePosition();
								protocolLength = length;
								
								data.skipBytes(length);
								break;
							case ID_SEC_WEBSOCKET_VERSION: //optional for the feed
								//only one version is supported
								isValidVersion = data.equalUTF(WS_VERSION_SUPPORTED);
								if (!isValidVersion) {
									data.skipBytes(data.readShort());
								}
								break;
							case ID_ORIGIN: //optional
								isExpectedOrigin = data.equalUTF(expectedOrigin);
								if (!isExpectedOrigin) {
									//do not comment out this, it has side effect of consuming the field.
									logger.warn("the origin {} did not match expected {}",data.readUTF(),new String(expectedOrigin));
								}
								break;							
						}
						id = data.readShort();
					}

					
					
					int fieldRevision = Pipe.takeInt(input);
					int fieldRequestContext = Pipe.takeInt(input);
					
					//something wrong with the request
	    	    	if (HTTPVerbDefaults.GET.ordinal()!=verbId
	    	    		|| (!isUpgradeWebsocket)
	    	    		|| (!isConnectionUpgrade)
	    	    		|| (!isExpectedOrigin)
	    	    		) {
	    	    		HTTPUtil.publishStatus(fieldChannelId, fieldSequence, 404, output);
	    	    	} else {
	    	    		writeResponse(output, fieldChannelId, fieldSequence, routeId
	    	    				     , acceptBacking, acceptPosition, acceptLength, acceptMask
	    	    				     , protocolBacking, protocolPosition, protocolLength, protocolMask);
	    	    	}
	    	    	
	    	    	//only mark as read after we have written the response.
			        Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
			        Pipe.releaseReadLock(input);
										
		            
		        break;
		        case -1:
		        	Pipe.publishEOF(output);
		        break;
		    }
		    PipeReader.releaseReadLock(input);
		}
	}

	private void writeResponse(Pipe<ServerResponseSchema> output
			                  , long fieldChannelId
			                  , int fieldSequence
			                  , int routeId
			                  , byte[] acceptBacking, int acceptPosition, int acceptLength, int acceptMask
			                  , byte[] protocolBacking, int protocolPosition, int protocolLength, int protocolMask) {
							///response ///////////////////////
							//			        HTTP/1.1 101 Switching Protocols
							//			        Upgrade: websocket
							//			        Connection: Upgrade
							//			        Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
							//			        Sec-WebSocket-Protocol: chat
							///////////////////////////////////
							
							
							int channelIdHigh = (int)(fieldChannelId>>32); 
							int channelIdLow = (int)fieldChannelId;		

							byte[] typeBytes = null;
							int headerSize = Pipe.addMsgIdx(output, ServerResponseSchema.MSG_TOCHANNEL_100); //channel, sequence, context, payload 
							
							Pipe.addIntValue(channelIdHigh, output);
							Pipe.addIntValue(channelIdLow, output);
							Pipe.addIntValue(fieldSequence, output);
							
							DataOutputBlobWriter<ServerResponseSchema> writer = Pipe.openOutputStream(output);        
										
							//line one
							writer.write(HTTPRevisionDefaults.HTTP_1_1.getBytes());
							writer.write(AbstractRestStage.Switching_Protocols_101);          
				       
							writer.write(HTTPHeaderDefaults.UPGRADE.rootBytes());
							writer.write(WS_WEBSOCKET);
							writer.write(AbstractRestStage.RETURN_NEWLINE);
							
							writer.write(HTTPHeaderDefaults.CONNECTION.rootBytes());
							writer.write(WS_UPGRADE);
							writer.write(AbstractRestStage.RETURN_NEWLINE);
							
							
							if (acceptLength>=0) {
								writer.write(HTTPHeaderDefaults.SEC_WEBSOCKET_ACCEPT.rootBytes());
								writer.write(acceptBacking, acceptPosition, acceptLength, acceptMask);
								writer.write(AbstractRestStage.RETURN_NEWLINE);
							}
							
							
							if (protocolLength>=0) {
								writer.write(HTTPHeaderDefaults.SEC_WEBSOCKET_PROTOCOL.rootBytes());
								writer.write(protocolBacking, protocolPosition, protocolLength, protocolMask);
								writer.write(AbstractRestStage.RETURN_NEWLINE);
							}
							
							
							
							//TODO: write these...
							//			        Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
							//			        Sec-WebSocket-Protocol: chat
							
							writer.write(AbstractRestStage.RETURN_NEWLINE);
							
							writer.closeLowLevelField();          
							
							//request context
							Pipe.addIntValue( ServerCoordinator.END_RESPONSE_MASK
											| ServerCoordinator.UPGRADE_MASK
											| (ServerCoordinator.UPGRADE_TARGET_PIPE_MASK & routeId) 
											, output); //empty request context, set the full value last.                        
							
							Pipe.confirmLowLevelWrite(output, headerSize);
							Pipe.publishWrites(output);
	}

}
