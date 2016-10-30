package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetRequestSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class HTTPClientRequestStage extends PronghornStage {

	public static final Logger log = LoggerFactory.getLogger(HTTPClientRequestStage.class);
	
	private final Pipe<NetRequestSchema>[] input;
	private final Pipe<NetPayloadSchema>[] output;
	private final ClientConnectionManager ccm;

	private final long disconnectTimeoutMS = 10_000;  //TODO: set with param
	private long nextUnusedCheck = 0;
	
	private int activeOutIdx = 0;
			
	private static final String implementationVersion = PronghornStage.class.getPackage().getImplementationVersion()==null?"unknown":PronghornStage.class.getPackage().getImplementationVersion();
		
	private StringBuilder activeHost;
	

	public HTTPClientRequestStage(GraphManager graphManager, 	
			ClientConnectionManager ccm,
            Pipe<NetRequestSchema>[] input,
            Pipe<NetPayloadSchema>[] output
            ) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
		this.ccm = ccm;
		
	}
	
	
	@Override
	public void startup() {
		super.startup();		
		this.activeHost = new StringBuilder();
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
		long now = System.currentTimeMillis();
		int i = input.length;
		while (--i>=0) {
			processMessagesForPipe(i, now);
		}
		
		//check if some connections have not been used and can be closed.
		if (now>nextUnusedCheck) {
			closeUnusedConnections();
			nextUnusedCheck = now+disconnectTimeoutMS;
		}
		
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
	
	protected void processMessagesForPipe(int activePipe, long now) {
		
		    Pipe<NetRequestSchema> requestPipe = input[activePipe];
		    	        	    		    
	        while (PipeReader.hasContentToRead(requestPipe)         
	                && hasRoomForWrite(requestPipe, activeHost, output, ccm)
	                && PipeReader.tryReadFragment(requestPipe) ){
	  	    
	        	
	        	//Need peek to know if this will block.
	        	
	            int msgIdx = PipeReader.getMsgIdx(requestPipe);
	            
				switch (msgIdx) {
							case -1:
								
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
								return;
								
								
	            			case NetRequestSchema.MSG_HTTPGET_100:
	            				
	            				activeHost.setLength(0);//NOTE: we may want to think about a zero copy design
				                PipeReader.readUTF8(requestPipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2, activeHost);
				                {
					                int port = PipeReader.readInt(requestPipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_PORT_1);
					                int userId = PipeReader.readInt(requestPipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_LISTENER_10);
					                
					                long connectionId = ccm.lookup(activeHost, port, userId);	
					                
					                if (-1 != connectionId) {
						                
					                	ClientConnection clientConnection = (ClientConnection)ccm.get(connectionId, 0);
					                	clientConnection.setLastUsedTime(now);
					                	int outIdx = clientConnection.requestPipeLineIdx();
					                	
					                	clientConnection.incRequestsSent();//count of messages can only be done here.
										Pipe<NetPayloadSchema> outputPipe = output[outIdx];
						                				                	
						                if (PipeWriter.tryWriteFragment(outputPipe, NetPayloadSchema.MSG_PLAIN_210) ) {
						                    	
						                	PipeWriter.writeLong(outputPipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, connectionId);
						                	
						                	DataOutputBlobWriter<NetPayloadSchema> activeWriter = PipeWriter.outputStream(outputPipe);
						                	DataOutputBlobWriter.openField(activeWriter);
											
						                	DataOutputBlobWriter.encodeAsUTF8(activeWriter,"GET");
						                	
						                	int len = PipeReader.readDataLength(requestPipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3);					                	
						                	int  first = PipeReader.readBytesPosition(requestPipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3);					                	
						                	boolean prePendSlash = (0==len) || ('/' != PipeReader.readBytesBackingArray(requestPipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3)[first&Pipe.blobMask(requestPipe)]);  
						                	
											if (prePendSlash) { //NOTE: these can be pre-coverted to bytes so we need not convert on each write. may want to improve.
												DataOutputBlobWriter.encodeAsUTF8(activeWriter," /");
											} else {
												DataOutputBlobWriter.encodeAsUTF8(activeWriter," ");
											}
											
											//Reading from UTF8 field and writing to UTF8 encoded field so we are doing a direct copy here.
											PipeReader.readBytes(requestPipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_PATH_3, activeWriter);
											
											finishWritingHeader(activeHost, activeWriter, implementationVersion, 0);
						                	DataOutputBlobWriter.closeHighLevelField(activeWriter, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
						                					                	
						                	PipeWriter.publishWrites(outputPipe);
						                					                	
						                } else {
						                	throw new RuntimeException("Unable to send request, outputPipe is full");
						                }
					                } else {
					                	
					                	//TODO: what is this case?
					               
					                	
					                }
			                	}
	            		break;
	            			case NetRequestSchema.MSG_HTTPPOST_101:
	            				
	            				
	            				activeHost.setLength(0);//NOTE: we may want to think about a zero copy design
				                PipeReader.readUTF8(requestPipe, NetRequestSchema.MSG_HTTPPOST_101_FIELD_HOST_2, activeHost);
				                {
					                int port = PipeReader.readInt(requestPipe, NetRequestSchema.MSG_HTTPPOST_101_FIELD_PORT_1);
					                int userId = PipeReader.readInt(requestPipe, NetRequestSchema.MSG_HTTPPOST_101_FIELD_LISTENER_10);
					                
					                long connectionId = ccm.lookup(activeHost, port, userId);	
					                //openConnection(activeHost, port, userId, outIdx);
					                
					                if (-1 != connectionId) {
						                
					                	ClientConnection clientConnection = (ClientConnection)ccm.get(connectionId, 0);
					                	clientConnection.setLastUsedTime(now);
					                	int outIdx = clientConnection.requestPipeLineIdx();
					                					                  	
					                	clientConnection.incRequestsSent();//count of messages can only be done here.
										Pipe<NetPayloadSchema> outputPipe = output[outIdx];
					                
						                if (PipeWriter.tryWriteFragment(outputPipe, NetPayloadSchema.MSG_PLAIN_210) ) {
					                    	
						                	PipeWriter.writeLong(outputPipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, connectionId);
						                	
						                	DataOutputBlobWriter<NetPayloadSchema> activeWriter = PipeWriter.outputStream(outputPipe);
						                	DataOutputBlobWriter.openField(activeWriter);
						                			                
						                	DataOutputBlobWriter.encodeAsUTF8(activeWriter,"POST");
						                	
						                	int len = PipeReader.readDataLength(requestPipe, NetRequestSchema.MSG_HTTPPOST_101_FIELD_PATH_3);					                	
						                	int  first = PipeReader.readBytesPosition(requestPipe, NetRequestSchema.MSG_HTTPPOST_101_FIELD_PATH_3);					                	
						                	boolean prePendSlash = (0==len) || ('/' != PipeReader.readBytesBackingArray(requestPipe, NetRequestSchema.MSG_HTTPPOST_101_FIELD_PATH_3)[first&Pipe.blobMask(requestPipe)]);  
						                	
											if (prePendSlash) { //NOTE: these can be pre-coverted to bytes so we need not convert on each write. may want to improve.
												DataOutputBlobWriter.encodeAsUTF8(activeWriter," /");
											} else {
												DataOutputBlobWriter.encodeAsUTF8(activeWriter," ");
											}
											
											//Reading from UTF8 field and writing to UTF8 encoded field so we are doing a direct copy here.
											PipeReader.readBytes(requestPipe, NetRequestSchema.MSG_HTTPPOST_101_FIELD_PATH_3, activeWriter);
											
											long length = PipeReader.readBytesLength(requestPipe, NetRequestSchema.MSG_HTTPPOST_101_FIELD_PAYLOAD_5);
											
											//For chunked must pass in -1

											//TODO: this field can no be any loger than 4G so we cant post anything larger than that
											//TODO: we also need support for chunking which will need multiple mesage fragments
											//TODO: need new message type for chunking/streaming post
											
											finishWritingHeader(activeHost, activeWriter, implementationVersion, length);
											
											PipeReader.readBytes(requestPipe, NetRequestSchema.MSG_HTTPPOST_101_FIELD_PAYLOAD_5, activeWriter);
											
						                	DataOutputBlobWriter.closeHighLevelField(activeWriter, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
						                					                	
						                	PipeWriter.publishWrites(outputPipe);
						                					                	
						                } else {
						                	System.err.println("unable to write");
						                	throw new RuntimeException("Unable to send request, outputPipe is full");
						                }
										
					                }
		            		
				                }
	    	        	break;	    	            	            	
	            
	            }
			
				PipeReader.releaseReadLock(requestPipe);				

	        }	            
		
	}


	private static void cleanCloseConnection(ClientConnection connectionToKill, Pipe<NetPayloadSchema> pipe) {
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


	public  boolean hasRoomForWrite(Pipe<NetRequestSchema> requestPipe, StringBuilder activeHost, Pipe<NetPayloadSchema>[] output, ClientConnectionManager ccm) {
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
		int outIdx = result;
		if (-1 == outIdx) {
			return false;
		}
		return hasOpenConnection(requestPipe, activeHost, output, ccm, outIdx);
	}


	public static boolean hasOpenConnection(Pipe<NetRequestSchema> requestPipe, StringBuilder activeHost,
											Pipe<NetPayloadSchema>[] output, ClientConnectionManager ccm, int outIdx) {
		activeHost.setLength(0);//NOTE: we may want to think about a zero copy design
		PipeReader.peekUTF8(requestPipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_HOST_2, activeHost);
		
		int port = PipeReader.peekInt(requestPipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_PORT_1);
		int userId = PipeReader.peekInt(requestPipe, NetRequestSchema.MSG_HTTPGET_100_FIELD_LISTENER_10);
				
		long connectionId = ClientConnectionManager.openConnection(ccm, activeHost, port, userId, outIdx);
		if (connectionId>=0) {
			ClientConnection clientConnection = (ClientConnection)ccm.get(connectionId, 0);
			//if we have a pre-existing pipe, must use it.
			outIdx = clientConnection.requestPipeLineIdx();
			if (!PipeWriter.hasRoomForWrite(output[outIdx])) {
				return false;
			}
		} else {
			 //"Has no room" for the new connection so we request that the oldest connection is closed.
			
			ClientConnection connectionToKill = (ClientConnection)ccm.get( -connectionId, 0);
			if (null!=connectionToKill) {
				Pipe<NetPayloadSchema> pipe = output[connectionToKill.requestPipeLineIdx()];
				if (PipeWriter.hasRoomForWrite(pipe)) {
					//close the least used connection
					cleanCloseConnection(connectionToKill, pipe);				
				}
			}
			return false;
		}
		return true;
	}

	public static void finishWritingHeader(CharSequence host, DataOutputBlobWriter<NetPayloadSchema> writer, CharSequence implementationVersion, long length) {
		DataOutputBlobWriter.encodeAsUTF8(writer," HTTP/1.1\r\nHost: ");
		DataOutputBlobWriter.encodeAsUTF8(writer,host);
		DataOutputBlobWriter.encodeAsUTF8(writer,"\r\nUser-Agent: Pronghorn/");
		DataOutputBlobWriter.encodeAsUTF8(writer,implementationVersion);
		if (length>0) {
			DataOutputBlobWriter.encodeAsUTF8(writer,"\r\nContent-Length: "+Long.toString(length)); //TODO: rewrite as garbage free.
		} else if (length<0) {
			DataOutputBlobWriter.encodeAsUTF8(writer,"\r\nTransfer-Encoding: chunked");//TODO: write the payload must be chunked.
		}
		DataOutputBlobWriter.encodeAsUTF8(writer,"\r\nConnection: keep-alive\r\n\r\n"); //double \r\b marks the end of the header
	}
	
	
	

}
