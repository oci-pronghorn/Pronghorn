package com.ociweb.pronghorn.network;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

public class HTTPClientRequestStage extends PronghornStage {

	public static final Logger logger = LoggerFactory.getLogger(HTTPClientRequestStage.class);
	
	private final Pipe<ClientHTTPRequestSchema>[] input;
	private final Pipe<NetPayloadSchema>[] output;
	private final ClientCoordinator ccm;

	private final long disconnectTimeoutMS = 10_000;  //TODO: set with param
	private long nextUnusedCheck = 0;
	
	private int activeOutIdx = 0;
			
	private static final String implementationVersion = PronghornStage.class.getPackage().getImplementationVersion()==null?"unknown":PronghornStage.class.getPackage().getImplementationVersion();
		
	private static final byte[] EMPTY = new byte[0];
	

	public HTTPClientRequestStage(GraphManager graphManager, 	
			ClientCoordinator ccm,
            Pipe<ClientHTTPRequestSchema>[] input,
            Pipe<NetPayloadSchema>[] output
            ) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
		this.ccm = ccm;
		
		//TODO: we have a bug here detecting EOF so this allows us to shutdown until its found.
		GraphManager.addNota(graphManager, GraphManager.PRODUCER, GraphManager.PRODUCER, this);
	}
	
	
	@Override
	public void startup() {
		
		super.startup();		
		
	}
	
	@Override
	public void shutdown() {
		
		int i = output.length;
		while (--i>=0) {
			PipeWriter.publishEOF(output[i]);
		}
	}
	
	@Override
	public void run() {
		boolean didWork;
		
		do {
			didWork = false;
			long now = System.currentTimeMillis();
			int i = input.length;
			while (--i>=0) {
				didWork |= processMessagesForPipe(i, now);
			}
			
			//check if some connections have not been used and can be closed.
			if (now>nextUnusedCheck) {
				//TODO: URGENT, this is killing of valid connections, but why? debug
				//	closeUnusedConnections();
				nextUnusedCheck = now+disconnectTimeoutMS;
			}
			
		} while (didWork);
		
	}


	private void closeUnusedConnections() {
		long now;
		ClientConnection con = ccm.nextValidConnection();
		final ClientConnection firstCon = con;					
		while (null!=con) {
			con = ccm.nextValidConnection();
			
			long unused = now = con.getLastUsedTime();
			
			if (unused>disconnectTimeoutMS) {
				
				Pipe<NetPayloadSchema> pipe = output[con.requestPipeLineIdx()];
				if (PipeWriter.hasRoomForWrite(pipe)) {
					//close the least used connection
					cleanCloseConnection(con, pipe);				
				}
				
			}
			
			if (firstCon==con) {
				break;
			}
		}
	}
	
	protected boolean processMessagesForPipe(int activePipe, long now) {
		
		
		    Pipe<ClientHTTPRequestSchema> requestPipe = input[activePipe];
		    	  
		    boolean didWork = false;

//			    	
//		    logger.info("top of whiile {} {} {} {}",
//		    		PipeReader.hasContentToRead(requestPipe)
//		    		   );
		    
		    
	        if (PipeReader.hasContentToRead(requestPipe)         
	             && hasOpenConnection(requestPipe, output, ccm, findAPipeWithRoom(output))
	             && PipeReader.tryReadFragment(requestPipe) ){
	  	    	        	
	        	//Need peek to know if this will block.
	 	        		        	
	            int msgIdx = PipeReader.getMsgIdx(requestPipe);
	            
	            didWork = true;
	        
				switch (msgIdx) {
							case -1:
								logger.info("Received shutdown message");
								
								ClientConnection connectionToKill = ccm.nextValidConnection();
								final ClientConnection firstToKill = connectionToKill;					
								while (null!=connectionToKill) {								
									connectionToKill = ccm.nextValidConnection();
									
									//must send handshake request down this pipe
									int pipeId = connectionToKill.requestPipeLineIdx();
									
									cleanCloseConnection(connectionToKill, output[pipeId]);
																		
									if (firstToKill == connectionToKill) {
										break;//done
									}
								}
								
								requestShutdown();
								PipeReader.releaseReadLock(requestPipe);
								return false;
								
							case ClientHTTPRequestSchema.MSG_CLOSE_104:
							{								
								int pipeId = activeConnection.requestPipeLineIdx();
								cleanCloseConnection(activeConnection, output[pipeId]);
								
							}	
		                	break;		
	            			case ClientHTTPRequestSchema.MSG_FASTHTTPGET_200:
	    	            		
			                {
					                
				                	//logger.info("request sent to connection id {} for host {}, port {}, userid {} ",connectionId, activeHost, port, userId);
				                	
				                	ClientConnection clientConnection = activeConnection;
				  
					                	clientConnection.setLastUsedTime(now);
					                	int outIdx = clientConnection.requestPipeLineIdx();
					                	
					                	//logger.info("sent get request down pipe {} ",outIdx);
					                	
					                	clientConnection.incRequestsSent();//count of messages can only be done here.
										Pipe<NetPayloadSchema> outputPipe = output[outIdx];
						                				                	
						                if (PipeWriter.tryWriteFragment(outputPipe, NetPayloadSchema.MSG_PLAIN_210) ) {
						                    	
						                	PipeWriter.writeLong(outputPipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, clientConnection.id);
						                 	PipeWriter.writeLong(outputPipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, 0);
						                 	
						                 	
						                	DataOutputBlobWriter<NetPayloadSchema> activeWriter = PipeWriter.outputStream(outputPipe);
						                	DataOutputBlobWriter.openField(activeWriter);
											
						                	DataOutputBlobWriter.encodeAsUTF8(activeWriter,"GET");
						                	
						                	int len = PipeReader.readBytesLength(requestPipe, ClientHTTPRequestSchema.MSG_FASTHTTPGET_200_FIELD_PATH_3);					                	
						                	int  first = PipeReader.readBytesPosition(requestPipe, ClientHTTPRequestSchema.MSG_FASTHTTPGET_200_FIELD_PATH_3);					                	
						                	boolean prePendSlash = (0==len) || ('/' != PipeReader.readBytesBackingArray(requestPipe, ClientHTTPRequestSchema.MSG_FASTHTTPGET_200_FIELD_PATH_3)[first&Pipe.blobMask(requestPipe)]);  
						                	
											if (prePendSlash) { //NOTE: these can be pre-coverted to bytes so we need not convert on each write. may want to improve.
												DataOutputBlobWriter.encodeAsUTF8(activeWriter," /");
											} else {
												DataOutputBlobWriter.encodeAsUTF8(activeWriter," ");
											}
											
											//Reading from UTF8 field and writing to UTF8 encoded field so we are doing a direct copy here.
											PipeReader.readBytes(requestPipe, ClientHTTPRequestSchema.MSG_FASTHTTPGET_200_FIELD_PATH_3, activeWriter);
											
						            		final byte[] hostBack = PipeReader.readBytesBackingArray(requestPipe, ClientHTTPRequestSchema.MSG_FASTHTTPGET_200_FIELD_HOST_2);
						            		final int hostPos = PipeReader.readBytesPosition(requestPipe, ClientHTTPRequestSchema.MSG_FASTHTTPGET_200_FIELD_HOST_2);
						            		final int hostLen = PipeReader.readBytesLength(requestPipe, ClientHTTPRequestSchema.MSG_FASTHTTPGET_200_FIELD_HOST_2);
						            		final int hostMask = Pipe.blobMask(requestPipe);	
											
											finishWritingHeader(hostBack, hostPos, hostLen, hostMask, activeWriter, implementationVersion, 0);
						                	DataOutputBlobWriter.closeHighLevelField(activeWriter, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
						                					                	
						                	PipeWriter.publishWrites(outputPipe);
						                	
						                    //logger.info("published the get request {}",outputPipe);
						                	
						                					                	
						                } else {
						                	throw new RuntimeException("Unable to send request, outputPipe is full");
						                }
						           
					
		                	}
            		   break;
	            	   case ClientHTTPRequestSchema.MSG_HTTPGET_100:
	            		
				                {
						                
					                	//logger.info("request sent to connection id {} for host {}, port {}, userid {} ",connectionId, activeHost, port, userId);
					                	
					                	ClientConnection clientConnection = activeConnection;
					  
						                	clientConnection.setLastUsedTime(now);
						                	int outIdx = clientConnection.requestPipeLineIdx();
						                	
						                	//logger.info("sent get request down pipe {} ",outIdx);
						                	
						                	clientConnection.incRequestsSent();//count of messages can only be done here.
											Pipe<NetPayloadSchema> outputPipe = output[outIdx];
							                				                	
							                if (PipeWriter.tryWriteFragment(outputPipe, NetPayloadSchema.MSG_PLAIN_210) ) {
							                    	
							                	PipeWriter.writeLong(outputPipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, clientConnection.id);
							                 	PipeWriter.writeLong(outputPipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, 0);
							                 	
							                 	
							                	DataOutputBlobWriter<NetPayloadSchema> activeWriter = PipeWriter.outputStream(outputPipe);
							                	DataOutputBlobWriter.openField(activeWriter);
												
							                	DataOutputBlobWriter.encodeAsUTF8(activeWriter,"GET");
							                	
							                	int len = PipeReader.readBytesLength(requestPipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3);					                	
							                	int  first = PipeReader.readBytesPosition(requestPipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3);					                	
							                	boolean prePendSlash = (0==len) || ('/' != PipeReader.readBytesBackingArray(requestPipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3)[first&Pipe.blobMask(requestPipe)]);  
							                	
												if (prePendSlash) { //NOTE: these can be pre-coverted to bytes so we need not convert on each write. may want to improve.
													DataOutputBlobWriter.encodeAsUTF8(activeWriter," /");
												} else {
													DataOutputBlobWriter.encodeAsUTF8(activeWriter," ");
												}
												
												//Reading from UTF8 field and writing to UTF8 encoded field so we are doing a direct copy here.
												PipeReader.readBytes(requestPipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3, activeWriter);
												
							            		final byte[] hostBack = PipeReader.readBytesBackingArray(requestPipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2);
							            		final int hostPos = PipeReader.readBytesPosition(requestPipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2);
							            		final int hostLen = PipeReader.readBytesLength(requestPipe, ClientHTTPRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2);
							            		final int hostMask = Pipe.blobMask(requestPipe);	
												
												finishWritingHeader(hostBack, hostPos, hostLen, hostMask, activeWriter, implementationVersion, 0);
							                	DataOutputBlobWriter.closeHighLevelField(activeWriter, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
							                					                	
							                	PipeWriter.publishWrites(outputPipe);
							                	
							                    //logger.info("published the get request {}",outputPipe);
							                	
							                					                	
							                } else {
							                	throw new RuntimeException("Unable to send request, outputPipe is full");
							                }
							           
						
			                	}
	            		   break;
	            			case ClientHTTPRequestSchema.MSG_HTTPPOST_101:
	            			
				                {
			                
					        
					                	ClientConnection clientConnection = activeConnection;
					                	clientConnection.setLastUsedTime(now);
					                	int outIdx = clientConnection.requestPipeLineIdx();
					                					                  	
					                	clientConnection.incRequestsSent();//count of messages can only be done here.
										Pipe<NetPayloadSchema> outputPipe = output[outIdx];
					                
						                if (PipeWriter.tryWriteFragment(outputPipe, NetPayloadSchema.MSG_PLAIN_210) ) {
					                    	
						                	PipeWriter.writeLong(outputPipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, clientConnection.id);
						                	
						                	DataOutputBlobWriter<NetPayloadSchema> activeWriter = PipeWriter.outputStream(outputPipe);
						                	DataOutputBlobWriter.openField(activeWriter);
						                			                
						                	DataOutputBlobWriter.encodeAsUTF8(activeWriter,"POST");
						                	
						                	int len = PipeReader.readBytesLength(requestPipe,ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_PATH_3);					                	
						                	int  first = PipeReader.readBytesPosition(requestPipe, ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_PATH_3);					                	
						                	boolean prePendSlash = (0==len) || ('/' != PipeReader.readBytesBackingArray(requestPipe, ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_PATH_3)[first&Pipe.blobMask(requestPipe)]);  
						                	
											if (prePendSlash) { //NOTE: these can be pre-coverted to bytes so we need not convert on each write. may want to improve.
												DataOutputBlobWriter.encodeAsUTF8(activeWriter," /");
											} else {
												DataOutputBlobWriter.encodeAsUTF8(activeWriter," ");
											}
											
											//Reading from UTF8 field and writing to UTF8 encoded field so we are doing a direct copy here.
											PipeReader.readBytes(requestPipe, ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_PATH_3, activeWriter);
											
											long length = PipeReader.readBytesLength(requestPipe, ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_PAYLOAD_5);
											
											//For chunked must pass in -1

											//TODO: this field can no be any loger than 4G so we cant post anything larger than that
											//TODO: we also need support for chunking which will need multiple mesage fragments
											//TODO: need new message type for chunking/streaming post
											
						            		final byte[] hostBack = PipeReader.readBytesBackingArray(requestPipe, ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_HOST_2);
						            		final int hostPos = PipeReader.readBytesPosition(requestPipe, ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_HOST_2);
						            		final int hostLen = PipeReader.readBytesLength(requestPipe, ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_HOST_2);
						            		final int hostMask = Pipe.blobMask(requestPipe);
											
											finishWritingHeader(hostBack, hostPos, hostLen, hostMask, activeWriter, implementationVersion, length);
											
											PipeReader.readBytes(requestPipe, ClientHTTPRequestSchema.MSG_HTTPPOST_101_FIELD_PAYLOAD_5, activeWriter);
											
						                	DataOutputBlobWriter.closeHighLevelField(activeWriter, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
						                					                	
						                	PipeWriter.publishWrites(outputPipe);
						                					                	
						                } else {
						                	System.err.println("unable to write");
						                	throw new RuntimeException("Unable to send request, outputPipe is full");
						                }
										
		            		
				                }
	    	           	    break;	 
	    	           	    default:
	    	           	    	throw new UnsupportedOperationException("Unexpected Message Idx");
	            
	            }
			
				PipeReader.releaseReadLock(requestPipe);	


	        }	            
		return didWork;
	}


	private static void cleanCloseConnection(ClientConnection connectionToKill, Pipe<NetPayloadSchema> pipe) {
		
		//logger.info("CLIENT SIDE BEGIN CONNECTION CLOSE");

		//do not close that will be done by last stage
		//must be done first before we send the message
		connectionToKill.beginDisconnect();

		if (PipeWriter.tryWriteFragment(pipe, NetPayloadSchema.MSG_DISCONNECT_203) ) {
		    PipeWriter.writeLong(pipe, NetPayloadSchema.MSG_DISCONNECT_203_FIELD_CONNECTIONID_201, connectionToKill.getId());
			PipeWriter.publishWrites(pipe);
		} else {
			throw new RuntimeException("Unable to send request, outputPipe is full");
		}
	}


	private int findAPipeWithRoom(Pipe<NetPayloadSchema>[] output) {
		int result = -1;
		//if we go around once and find nothing then stop looking
		int i = output.length;
		while (--i>=0) {
			//next idx		
			if (++activeOutIdx == output.length) {
				activeOutIdx = 0;
			}
			//does this one have room
			if (PipeWriter.hasRoomForWrite(output[activeOutIdx])) {
				result = activeOutIdx;
				break;
			}
		}
		return result;
	}


	ClientConnection activeConnection =  null;
	
	//has side effect fo storing the active connectino as a member so it neeed not be looked up again later.
	private boolean hasOpenConnection(Pipe<ClientHTTPRequestSchema> requestPipe, 
											Pipe<NetPayloadSchema>[] output, ClientCoordinator ccm, int outIdx) {

		int msgIdx = Pipe.peekInt(requestPipe);
		
		
		if (Pipe.peekMsg(requestPipe, -1)) {
			return hasRoomForEOF(output);
		}

		assert (msgIdx==ClientHTTPRequestSchema.MSG_FASTHTTPGET_200 || msgIdx==ClientHTTPRequestSchema.MSG_HTTPGET_100 ) : "bad msgIdx of "+msgIdx+" at "+Pipe.getWorkingTailPosition(requestPipe)+"  "+requestPipe;
		
		int userId = Pipe.peekInt(requestPipe,   1); //user id always after the msg idx
		int port = Pipe.peekInt(requestPipe,     2); //port is always after the userId; 
			
		int hostMeta = Pipe.peekInt(requestPipe, 3); //host is always after port
 		int hostLen  = Pipe.peekInt(requestPipe, 4); //host is always after port

 		int hostPos  = Pipe.convertToPosition(hostMeta, requestPipe);		
 		byte[] hostBack = Pipe.byteBackingArray(hostMeta, requestPipe);
 		int hostMask = Pipe.blobMask(requestPipe);
 		
 		long connectionId;

 		if (Pipe.peekMsg(requestPipe, ClientHTTPRequestSchema.MSG_FASTHTTPGET_200)) {
 			connectionId = Pipe.peekLong(requestPipe, 5);//do not do lookup if it was already provided.
 		//	System.err.println("loaded connection "+connectionId);
 		} else {
     		connectionId = ccm.lookup(hostBack,hostPos,hostLen,hostMask, port, userId);
	//		System.err.println("first lookup connection "+connectionId);
 		}
		
		activeConnection = ClientCoordinator.openConnection(ccm, hostBack, hostPos, hostLen, hostMask, port, userId, outIdx, output, connectionId);
				
		
		if (null != activeConnection) {
			
			if (ccm.isTLS) {
				
				//If this connection needs to complete a hanshake first then do that and do not send the request content yet.
				HandshakeStatus handshakeStatus = activeConnection.getEngine().getHandshakeStatus();
				if (HandshakeStatus.FINISHED!=handshakeStatus && HandshakeStatus.NOT_HANDSHAKING!=handshakeStatus) {
					activeConnection = null;
					return false;
				}
	
			}
			
		} else {
			//this happens often when the profiler is running due to contention for sockets.
			
			//"Has no room" for the new connection so we request that the oldest connection is closed.
			
			//instead of doing this (which does not work) we will just wait by returning false.
//			ClientConnection connectionToKill = (ClientConnection)ccm.get( -connectionId, 0);
//			if (null!=connectionToKill) {
//				Pipe<NetPayloadSchema> pipe = output[connectionToKill.requestPipeLineIdx()];
//				if (PipeWriter.hasRoomForWrite(pipe)) {
//					//close the least used connection
//					cleanCloseConnection(connectionToKill, pipe);				
//				}
//			}
		
			return false;
		}
		
		
		outIdx = activeConnection.requestPipeLineIdx(); //this should be done AFTER any handshake logic
		Pipe<NetPayloadSchema> pipe = output[outIdx];
		if (!PipeWriter.hasRoomForWrite(pipe)) {
			return false;
		}
		return true;
	}


	public static boolean hasRoomForEOF(Pipe<NetPayloadSchema>[] output) {
		//all outputs must have room for EOF processing
		int i = output.length;
		while (--i>=0) {
			if (!PipeWriter.hasRoomForWrite(output[i])) {
				return false;
			}
		}
		return true;
	}

	private final static byte[] REV11_AND_HOST = " HTTP/1.1\r\nHost: ".getBytes();
	private final static byte[] LINE_AND_USER_AGENT = "\r\nUser-Agent: Pronghorn/".getBytes();	
	private final static byte[] CONNECTION_KEEP_ALIVE_END = "\r\nConnection: keep-alive\r\n\r\n".getBytes();
	private final static byte[] CONNECTION_CLOSE_END = "\r\nConnection: close\r\n\r\n".getBytes();
	
	
	public static void finishWritingHeader(byte[] hostBack, int hostPos, int hostLen, int hostMask,
			                               DataOutputBlobWriter<NetPayloadSchema> writer, CharSequence implementationVersion, long length) {
		DataOutputBlobWriter.write(writer, REV11_AND_HOST, 0, REV11_AND_HOST.length, Integer.MAX_VALUE); //encodeAsUTF8(writer," HTTP/1.1\r\nHost: ");
		DataOutputBlobWriter.write(writer,hostBack,hostPos,hostLen,hostMask);
		
	//	DataOutputBlobWriter.write(writer, LINE_AND_USER_AGENT, 0, LINE_AND_USER_AGENT.length, Integer.MAX_VALUE);//DataOutputBlobWriter.encodeAsUTF8(writer,"\r\nUser-Agent: Pronghorn/");
	//	DataOutputBlobWriter.encodeAsUTF8(writer,implementationVersion);

		if (length>0) {
			
			Appendables.appendValue(writer.append("\r\nContent-Length: "), length); //does the same as below...			
			//DataOutputBlobWriter.encodeAsUTF8(writer,"\r\nContent-Length: "+Long.toString(length));
		} else if (length<0) {
			DataOutputBlobWriter.encodeAsUTF8(writer,"\r\nTransfer-Encoding: chunked");//TODO: write the payload must be chunked.
		}
		
		DataOutputBlobWriter.write(writer, CONNECTION_KEEP_ALIVE_END, 0, CONNECTION_KEEP_ALIVE_END.length, Integer.MAX_VALUE);//DataOutputBlobWriter.encodeAsUTF8(writer,"\r\nConnection: keep-alive\r\n\r\n"); //double \r\b marks the end of the header
	
		//TODO: is server closing too early, need to send response first?
	//	DataOutputBlobWriter.write(writer, CONNECTION_CLOSE_END, 0, CONNECTION_CLOSE_END.length, Integer.MAX_VALUE);
		
	}
	
	
	

}
