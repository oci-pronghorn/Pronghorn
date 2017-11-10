package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerb;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.http.AbstractRestStage;
import com.ociweb.pronghorn.network.schema.HTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class UpgradeToWebSocketStage extends PronghornStage {

	private final Pipe<HTTPRequestSchema>[] inputPipes;
	private final Pipe<ServerResponseSchema>[] outputs;
	private final HTTPSpecification<?,?,?,?> httpSpec;
	
	private static final int ID_UPGRADE = 1;
	private static final int ID_CONNECTION = 2;
	private static final int ID_SEC_WEBSOCKET_KEY = 21;
	private static final int ID_SEC_WEBSOCKET_PROTOCOL = 22;
	private static final int ID_SEC_WEBSOCKET_VERSION = 23;
	private static final int ID_ORIGIN = 25;
		
	
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
		assert(httpSpec.headerMatches(ID_ORIGIN, HTTPHeaderDefaults.ORIGIN.writingRoot()));
		
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

		while (Pipe.hasContentToRead(input)) {
			
		    int msgIdx = Pipe.takeMsgIdx(input);
		    switch(msgIdx) {
		    	//TODO: this stage is going to get websocket inputs after upgrade
		        //TODO; this websocket upgrade logic should be in a common utility class.
		        //      each websocket frame comes in but may interleave with others...
		        //TODO: we only want large blocks for index on header, no others.
		        //TODO: need a simple stage which talks to MQTT from here??
		    
		    	case HTTPRequestSchema.MSG_WEBSOCKETFRAME_100:
		    	
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
					
	    	    	
	    	    	///TODO: keep the route and store for this upgrade,
					//TODO: confirm the expected verb... GET
					
	    	    	if (HTTPVerbDefaults.GET.ordinal()!=verbId) {
	    	    		//error
	    	    		
	    	    	}
	    	    	
					DataInputBlobReader<HTTPRequestSchema> data = Pipe.openInputStream(input);
				
					int id = data.readShort();
					while (id > 0) {
						switch(id) {
							case ID_UPGRADE: //required
								if (!data.equalUTF("websocket".getBytes())) {
									System.err.println("report new error value must be websocket");
								}
								break;
							case ID_CONNECTION: //required
								if (!data.equalUTF("Upgrade".getBytes())) {
									System.err.println("report new error value must be Upgrade");
								}
								break;
							case ID_SEC_WEBSOCKET_KEY: //optional used here for signature
								data.skipBytes(data.readShort());
								//data.readUTF(target);
								break;
							case ID_SEC_WEBSOCKET_PROTOCOL: //optional for the feed
								data.skipBytes(data.readShort());
								//data.readUTF(target);
								break;
							case ID_SEC_WEBSOCKET_VERSION: //optional for the feed
								data.skipBytes(data.readShort());
								//data.readUTF(target);
								break;
							case ID_ORIGIN: //optional
								data.skipBytes(data.readShort());
								//data.readUTF(target);
								break;							
						}
						id = data.readShort();
					}
			
					
					int fieldRevision = Pipe.takeInt(input);
					int fieldRequestContext = Pipe.takeInt(input);
					
					
					writeResponse(output, fieldChannelId, fieldSequence);
						
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

	private void writeResponse(Pipe<ServerResponseSchema> output, long fieldChannelId, int fieldSequence) {
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
							int requestContext = ServerCoordinator.END_RESPONSE_MASK;
																					
							int headerSize = Pipe.addMsgIdx(output, ServerResponseSchema.MSG_TOCHANNEL_100); //channel, sequence, context, payload 
							
							Pipe.addIntValue(channelIdHigh, output);
							Pipe.addIntValue(channelIdLow, output);
							Pipe.addIntValue(fieldSequence, output);
							
							DataOutputBlobWriter<ServerResponseSchema> writer = Pipe.openOutputStream(output);        
										
							//line one
							writer.write(HTTPRevisionDefaults.HTTP_1_1.getBytes());
							writer.write(AbstractRestStage.Switching_Protocols_101);          
				       
							writer.write(HTTPHeaderDefaults.UPGRADE.rootBytes());
							writer.write("websocket".getBytes());
							writer.write(AbstractRestStage.RETURN_NEWLINE);
							
							writer.write(HTTPHeaderDefaults.CONNECTION.rootBytes());
							writer.write("Upgrade".getBytes());
							writer.write(AbstractRestStage.RETURN_NEWLINE);
							
							
							//TODO: write these...
							//			        Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
							//			        Sec-WebSocket-Protocol: chat
							
							writer.write(AbstractRestStage.RETURN_NEWLINE);
							
							writer.closeLowLevelField();          
							
							Pipe.addIntValue(requestContext , output); //empty request context, set the full value last.                        
							
							Pipe.confirmLowLevelWrite(output, headerSize);
							Pipe.publishWrites(output);
	}

}
