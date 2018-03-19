package com.ociweb.pronghorn.network.http;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeUTF8MutableCharSquence;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class HTTPClientRequestStage extends PronghornStage {

	public static final Logger logger = LoggerFactory.getLogger(HTTPClientRequestStage.class);
	
	private final Pipe<ClientHTTPRequestSchema>[] input;
	private final Pipe<NetPayloadSchema>[] output;
	private final ClientCoordinator ccm;

	private final long disconnectTimeoutMS = 10_000;  //TODO: set with param
	private long nextUnusedCheck = 0;
				
	static final String implementationVersion = PronghornStage.class.getPackage().getImplementationVersion()==null?"unknown":PronghornStage.class.getPackage().getImplementationVersion();
	
	static final byte[] GET_BYTES_SPACE = "GET ".getBytes();
	static final byte[] GET_BYTES_SPACE_SLASH = "GET /".getBytes();
	
    private boolean shutdownInProgress;	
	
    public static HTTPClientRequestStage newInstance(GraphManager graphManager, 	
													ClientCoordinator ccm,
										            Pipe<ClientHTTPRequestSchema>[] input,
										            Pipe<NetPayloadSchema>[] output) {
    	return new HTTPClientRequestStage(graphManager, ccm, input, output);
    }
    
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
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lavenderblush", this);
	}
	
	
	@Override
	public void startup() {
		
		super.startup();		
		
	}
	
	@Override
	public void shutdown() {
		
		int i = output.length;
		while (--i>=0) {
				if (null!=output[i] && Pipe.isInit(output[i])) {
					Pipe.publishEOF(output[i]);
				}

		}
	}
	
	@Override
	public void run() {


		   	 if(shutdownInProgress) {
		    	 int i = output.length;
		         while (--i >= 0) {
		         	if (null!=output[i] && Pipe.isInit(output[i])) {
		         		if (!Pipe.hasRoomForWrite(output[i], Pipe.EOF_SIZE)){ 
		         			return;
		         		}  
		         	}
		         }
		         requestShutdown();
		         return;
			 }
			
			boolean hasWork;
			
			final long now = System.currentTimeMillis();
	
			do {
				hasWork = false;
				int i = input.length;
				while (--i>=0) {
					Pipe<ClientHTTPRequestSchema> requestPipe = input[i];						  
					if (Pipe.hasContentToRead(requestPipe)) {
						if (buildClientRequest(now, requestPipe)) {
							hasWork = true;
						} else {
							//try again later
							return; //blocked connection, handshake wait or output pipe full
						}
					}
				}
				
				//check if some connections have not been used and can be closed.
				if (now>nextUnusedCheck) {
					//TODO: URGENT, this is killing of valid connections, but why? debug
					//	closeUnusedConnections();
					nextUnusedCheck = now+disconnectTimeoutMS;
				}
		
			} while (hasWork);
			
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
				if (Pipe.hasRoomForWrite(pipe)) {
					//close the least used connection
					HTTPClientUtil.cleanCloseConnection(con, pipe);				
				}
				
			}
			
			if (firstCon==con) {
				break;
			}
		}
	}
	
	private boolean buildClientRequest(long now, Pipe<ClientHTTPRequestSchema> requestPipe) {
		boolean didWork = false;
		//This check is required when TLS is in use.
		if (isConnectionReadyForUse(requestPipe) ){
			didWork = true;	        
			
		       	//Need peek to know if this will block.
		    		        	
		    final int msgIdx = Pipe.takeMsgIdx(requestPipe);
		    
		   // logger.info("send for active pipe {} with msg {}",requestPipe.id,msgIdx);
		    
		    if (ClientHTTPRequestSchema.MSG_FASTHTTPGET_200 == msgIdx) {
		    	activeConnection.setLastUsedTime(now);
				HTTPClientUtil.publishGet(requestPipe, activeConnection, output[activeConnection.requestPipeLineIdx()], now, stageId);
		    } else  if (ClientHTTPRequestSchema.MSG_HTTPGET_100 == msgIdx) {
		    	//logger.info("Warning slower call for HTTP GET detected, clean up lazy init.");
		    	HTTPClientUtil.processGetLogic(now, requestPipe, activeConnection, output[activeConnection.requestPipeLineIdx()], stageId);
		    } else  if (ClientHTTPRequestSchema.MSG_HTTPPOST_101 == msgIdx) {
		    	HTTPClientUtil.processPostLogic(now, requestPipe, activeConnection, output[activeConnection.requestPipeLineIdx()], stageId);	            	
		    } else  if (ClientHTTPRequestSchema.MSG_CLOSE_104 == msgIdx) {
		    	HTTPClientUtil.cleanCloseConnection(activeConnection, output[activeConnection.requestPipeLineIdx()]);
		    } else  if (-1 == msgIdx) {
		    	//logger.info("Received shutdown message");								
				processShutdownLogic(requestPipe);
				return false;
		    } else {
		    	throw new UnsupportedOperationException("Unexpected Message Idx");
		    }		
			
			Pipe.confirmLowLevelRead(requestPipe, Pipe.sizeOf(ClientHTTPRequestSchema.instance, msgIdx));
			Pipe.releaseReadLock(requestPipe);	
     
		} 
		
		return didWork;
	}


	private void processShutdownLogic(Pipe<ClientHTTPRequestSchema> requestPipe) {
		ClientConnection connectionToKill = ccm.nextValidConnection();
		final ClientConnection firstToKill = connectionToKill;					
		while (null!=connectionToKill) {								
			connectionToKill = ccm.nextValidConnection();
			
			//must send handshake request down this pipe
			int pipeId = connectionToKill.requestPipeLineIdx();
			
			HTTPClientUtil.cleanCloseConnection(connectionToKill, output[pipeId]);
												
			if (firstToKill == connectionToKill) {
				break;//done
			}
		}
		
		shutdownInProgress = true;
		Pipe.confirmLowLevelRead(requestPipe, Pipe.EOF_SIZE);
		Pipe.releaseReadLock(requestPipe);
	}


	private ClientConnection activeConnection =  null;
	private PipeUTF8MutableCharSquence mCharSequence = new PipeUTF8MutableCharSquence();
	
	//has side effect of storing the active connection as a member so it need not be looked up again later.
	private boolean isConnectionReadyForUse(Pipe<ClientHTTPRequestSchema> requestPipe) {

		int msgIdx = Pipe.peekInt(requestPipe);
		
		if (Pipe.peekMsg(requestPipe, -1)) {
			return hasRoomForEOF(output);
		}
		
		int userId=0;
		int port=0;			
		int hostMeta=0;
 		int hostLen=0;
 		int hostPos=0;		
 		byte[] hostBack=null;
 		int hostMask=0;
 		
 		long connectionId;

 		if (Pipe.peekMsg(requestPipe, ClientHTTPRequestSchema.MSG_FASTHTTPGET_200)) {
 			connectionId = Pipe.peekLong(requestPipe, 6);//do not do lookup if it was already provided.
 			assert(-1 != connectionId);
 		} else {
 			
 			int routeId = Pipe.peekInt(requestPipe, 1);
 			userId = Pipe.peekInt(requestPipe,      2); //user id always after the msg idx
 			port = Pipe.peekInt(requestPipe,        3); //port is always after the userId; 
 			hostMeta = Pipe.peekInt(requestPipe,    4); //host is always after port
 	 		hostLen  = Pipe.peekInt(requestPipe,    5); //host is always after port
 	 		
 	 		hostPos  = Pipe.convertToPosition(hostMeta, requestPipe);		
 	 		hostBack = Pipe.byteBackingArray(hostMeta, requestPipe);
 	 		hostMask = Pipe.blobMask(requestPipe);
 			
     		connectionId = ccm.lookup(ccm.lookupHostId(mCharSequence.setToField(requestPipe, hostMeta, hostLen)), port, userId);
			System.err.println("first lookup connection "+connectionId);
 		}
		
 		if (null!=activeConnection && activeConnection.getId()==connectionId) {
 			//logger.info("this is the same connection we just used so no need to look it up");
 		} else {
 			if (0==port) {
 				int routeId = Pipe.peekInt(requestPipe, 1);
 	 			userId   = Pipe.peekInt(requestPipe,    2); //user id always after the msg idx
 	 			port     = Pipe.peekInt(requestPipe,    3); //port is always after the userId; 
 	 			hostMeta = Pipe.peekInt(requestPipe,    4); //host is always after port
 	 	 		hostLen  = Pipe.peekInt(requestPipe,    5); //host is always after port
 	 	 		hostPos  = Pipe.convertToPosition(hostMeta, requestPipe);		
 	 	 		hostBack = Pipe.byteBackingArray(hostMeta, requestPipe);
 	 	 		hostMask = Pipe.blobMask(requestPipe);
 			}
 		
 			activeConnection = ClientCoordinator.openConnection(
 					 ccm, 
 					 mCharSequence.setToField(requestPipe, hostMeta, hostLen), 
 					 port, userId, output, connectionId);
 	
 		}
 		
		if (null != activeConnection) {
			
			if (activeConnection.isBusy()) {
				return false;//must try again later when the server has responded.
			}
			
			
			assert(activeConnection.isFinishConnect());
			
			if (ccm.isTLS) {				
				//If this connection needs to complete a hanshake first then do that and do not send the request content yet.
				HandshakeStatus handshakeStatus = activeConnection.getEngine().getHandshakeStatus();
				if (HandshakeStatus.FINISHED!=handshakeStatus && HandshakeStatus.NOT_HANDSHAKING!=handshakeStatus 
						/* && HandshakeStatus.NEED_WRAP!=handshakeStatus*/) {
					//logger.info("doing the shake, status is "+handshakeStatus+" "+connectionId+"  "+activeConnection.id);
					activeConnection = null;	
					return false;
				}
			}
			return Pipe.hasRoomForWrite(output[activeConnection.requestPipeLineIdx()]);
			
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
		
			//logger.info("no connection");
			return false;
		}
		
		
	}


	public static boolean hasRoomForEOF(Pipe<NetPayloadSchema>[] output) {
		//all outputs must have room for EOF processing
		int i = output.length;
		while (--i>=0) {
			if (!Pipe.hasRoomForWrite(output[i])) {
				return false;
			}
		}
		return true;
	}

	
	

}