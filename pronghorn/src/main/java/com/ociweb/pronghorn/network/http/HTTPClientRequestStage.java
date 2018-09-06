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
import com.ociweb.pronghorn.stage.scheduling.ElapsedTimeRecorder;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.struct.StructRegistry;

/**
 * Takes a HTTP client request and responds with a net payload using a TrieParserReader.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class HTTPClientRequestStage extends PronghornStage {


	public static final Logger logger = LoggerFactory.getLogger(HTTPClientRequestStage.class);
	
	private final Pipe<ClientHTTPRequestSchema>[] input;
	private final Pipe<NetPayloadSchema>[] output;
	private final ClientCoordinator ccm;
	
	static final String implementationVersion = PronghornStage.class.getPackage().getImplementationVersion()==null?"unknown":PronghornStage.class.getPackage().getImplementationVersion();
	
	static final byte[] GET_BYTES_SPACE = "GET ".getBytes();
	static final byte[] GET_BYTES_SPACE_SLASH = "GET /".getBytes();
	
	static long CYCLE_LIMIT_HANDSHAKE = 100_000L; //10_000 is common so we want something a couple orders bigger.
	
    private boolean shutdownInProgress;	
	
    public static HTTPClientRequestStage newInstance(GraphManager graphManager, 	
													ClientCoordinator ccm,
										            Pipe<ClientHTTPRequestSchema>[] input,
										            Pipe<NetPayloadSchema>[] output) {
    	return new HTTPClientRequestStage(graphManager, ccm, input, output);
    }

    /**
     *
     * @param graphManager
     * @param ccm
     * @param input _in_ Multiple HTTP client requests
     * @param output _out_ Multiple net payload responses
     */
	public HTTPClientRequestStage(GraphManager graphManager, 	
			ClientCoordinator ccm,
            Pipe<ClientHTTPRequestSchema>[] input,
            Pipe<NetPayloadSchema>[] output
            ) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
		this.ccm = ccm;
	
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lavenderblush", this);
		
		recordTypeData = graphManager.recordTypeData;
		
	}
	
	private final StructRegistry recordTypeData;
	private HTTPClientConnectionFactory ccf;
	
	@Override
	public void startup() {
		
		super.startup();		
		ccf = new HTTPClientConnectionFactory(recordTypeData);
		
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
			
			do {
				hasWork = false;
				int i = input.length;
				while (--i>=0) {										  
					if (Pipe.hasContentToRead(input[i])) {	
						if (buildClientRequest(input[i])) {
							hasWork = true;
						} else {
							return;
						}
					}
				}
		
			} while (hasWork);
			
	}

	
	private boolean buildClientRequest(Pipe<ClientHTTPRequestSchema> requestPipe) {
		boolean didWork = false;
		

		if (Pipe.peekMsg(requestPipe, -1)) {
			//logger.info("\n ^^^ end of file shutdown message");
			if (hasRoomForEOF(output)) {
				Pipe.skipNextFragment(requestPipe);
				shutdownInProgress = true;
				
			} 
			return true;
		}
		
		
		//This check is required when TLS is in use.
		//also must ensure connection is open before taking messages.
		if (isConnectionReadyForUse(requestPipe) && null!=activeConnection ) {
			didWork = true;	        
			
			final int msgIdx = Pipe.takeMsgIdx(requestPipe);			
		    //we have already checked for connection so now send the request

			final long now = System.nanoTime();
			activeConnection.setLastUsedTime(now);
	       	
	
		    if (ClientHTTPRequestSchema.MSG_FASTHTTPGET_200 == msgIdx) {
				HTTPClientUtil.publishGetFast(requestPipe, activeConnection, output[activeConnection.requestPipeLineIdx()], now, stageId);
		    } else  if (ClientHTTPRequestSchema.MSG_FASTHTTPPOST_201 == msgIdx) {
		    	HTTPClientUtil.processPostFast(now, requestPipe, activeConnection, output[activeConnection.requestPipeLineIdx()], stageId);
		    } else  if (ClientHTTPRequestSchema.MSG_HTTPGET_100 == msgIdx) {
		    	//logger.info("Warning slower call for HTTP GET detected, clean up lazy init.");
		    	HTTPClientUtil.processGetSlow(now, requestPipe, activeConnection, output[activeConnection.requestPipeLineIdx()], stageId);
		    } else  if (ClientHTTPRequestSchema.MSG_HTTPPOST_101 == msgIdx) {
		    	HTTPClientUtil.processPostSlow(now, requestPipe, activeConnection, output[activeConnection.requestPipeLineIdx()], stageId);	            	
		    } else  if (ClientHTTPRequestSchema.MSG_CLOSE_104 == msgIdx) {
		    	HTTPClientUtil.cleanCloseConnection(requestPipe, activeConnection, output[activeConnection.requestPipeLineIdx()]);
		    } else {
		    	throw new UnsupportedOperationException("Unexpected Message Idx");
		    }		
			
			Pipe.confirmLowLevelRead(requestPipe, Pipe.sizeOf(ClientHTTPRequestSchema.instance, msgIdx));
			Pipe.releaseReadLock(requestPipe);	
     
		}
		return didWork;
	}


	
	private ClientConnection activeConnection =  null;
	private PipeUTF8MutableCharSquence mCharSequence = new PipeUTF8MutableCharSquence();
	
	//has side effect of storing the active connection as a member so it need not be looked up again later.
	private boolean isConnectionReadyForUse(Pipe<ClientHTTPRequestSchema> requestPipe) {

		
		int sessionId=0;
		int port=0;			
		int hostMeta=0;
 		int hostLen=0;
 		int hostPos=0;		
 		byte[] hostBack=null;
 		int hostMask=0;
 		
 		long connectionId;
 		
 		if (Pipe.peekMsg(requestPipe, ClientHTTPRequestSchema.MSG_FASTHTTPGET_200) 
 			||Pipe.peekMsg(requestPipe, ClientHTTPRequestSchema.MSG_FASTHTTPPOST_201) ) {
 			connectionId = Pipe.peekLong(requestPipe, 6);//do not do lookup if it was already provided.
 			activeConnection = (ClientConnection) ccm.connectionObjForConnectionId(connectionId, false);
 			assert(-1 != connectionId);
 		} else {
 
 			if (Pipe.peekMsg(requestPipe, ClientHTTPRequestSchema.MSG_CLOSE_104) ) {
 	 			
 				sessionId = Pipe.peekInt(requestPipe,      1); //user id always after the msg idx
 	 			port = Pipe.peekInt(requestPipe,        2); //port is always after the userId; 
 	 			hostMeta = Pipe.peekInt(requestPipe,    3); //host is always after port
 	 	 		hostLen  = Pipe.peekInt(requestPipe,    4); //host is always after port
 	 	 		assert(sessionId!=0) : "sessionId must not be zero, MsgId:"+Pipe.peekInt(requestPipe);
 	 	 		
 	 	 		
 	 	 		hostPos  = Pipe.convertToPosition(hostMeta, requestPipe);		
 	 	 		hostBack = Pipe.byteBackingArray(hostMeta, requestPipe);
 	 	 		hostMask = Pipe.blobMask(requestPipe);
 	 	 		
 	 	 		connectionId = ccm.lookup(ClientCoordinator.lookupHostId(hostBack, hostPos, hostLen, hostMask), port, sessionId);
 			} else {
 				assert(Pipe.peekMsg(requestPipe, ClientHTTPRequestSchema.MSG_HTTPGET_100, 
 						                         ClientHTTPRequestSchema.MSG_HTTPPOST_101 )) : "unsupported msg "+Pipe.peekInt(requestPipe);
 	 			sessionId = Pipe.peekInt(requestPipe,      2); //user id always after the msg idx
 	 			port = Pipe.peekInt(requestPipe,        3); //port is always after the userId; 
 	 			hostMeta = Pipe.peekInt(requestPipe,    4); //host is always after port
 	 	 		hostLen  = Pipe.peekInt(requestPipe,    5); //host is always after port
 	 	 		assert(sessionId!=0) : "sessionId must not be zero, MsgId:"+Pipe.peekInt(requestPipe);
 	 	 		
 	 	 		//for post what about the payload field???
 	 	 		
 	 	 		hostPos  = Pipe.convertToPosition(hostMeta, requestPipe);		
 	 	 		hostBack = Pipe.byteBackingArray(hostMeta, requestPipe);
 	 	 		hostMask = Pipe.blobMask(requestPipe);
 	 						
 	     		connectionId = ccm.lookup(ClientCoordinator.lookupHostId(hostBack, hostPos, hostLen, hostMask), port, sessionId);
 			}
 		}
		
 		if (null!=activeConnection
 			&& activeConnection.getId()==connectionId 
 			&& activeConnection.isValid()) {
 			//logger.info("this is the same connection we just used so no need to look it up");
 		} else {
 		
 			if (null!=activeConnection && activeConnection.id==connectionId) {
 				//this is the only point where we can decompose since 
 				//we are creating a new active connection 				
 				ccm.removeConnection(activeConnection.id);
 			}
 			
 			
 			if (0==port) {
 				int routeId = Pipe.peekInt(requestPipe, 1);
 	 			sessionId   = Pipe.peekInt(requestPipe,    2); //user id always after the msg idx
 	 			port     = Pipe.peekInt(requestPipe,    3); //port is always after the userId; 
 	 			hostMeta = Pipe.peekInt(requestPipe,    4); //host is always after port
 	 	 		hostLen  = Pipe.peekInt(requestPipe,    5); //host is always after port
 	 	 		hostPos  = Pipe.convertToPosition(hostMeta, requestPipe);		
 	 	 		hostBack = Pipe.byteBackingArray(hostMeta, requestPipe);
 	 	 		hostMask = Pipe.blobMask(requestPipe);
 	 	 		assert(sessionId!=0) : "sessionId must not be zero";
 			}

			activeConnection = ClientCoordinator.openConnection(
 					 ccm, 
 					 mCharSequence.setToField(requestPipe, hostMeta, hostLen), 
 					 port, sessionId, output, connectionId, ccf);
 		}
 		
		if (null != activeConnection) {

			
			//logger.info("finish new connect {} ",activeConnection.isFinishConnect());
			
			assert(activeConnection.isFinishConnect());
			
			if (ccm.isTLS) {				
				//If this connection needs to complete a handshake first then do that and do not send the request content yet.
				//ALL the conent will be held here until the connection and its handshake is complete
				//If this takes too long we can open a "new" connection and do the handshake again without loosing any data.
				HandshakeStatus handshakeStatus = activeConnection.getEngine().getHandshakeStatus();
				if (HandshakeStatus.FINISHED!=handshakeStatus && HandshakeStatus.NOT_HANDSHAKING!=handshakeStatus
						) {
			
					if (++blockedCycles>=CYCLE_LIMIT_HANDSHAKE) { //are often 10_000 without error.
						//if this is new connection but it is not hand shaking as expected 
						//then we will drop the connection and try again.
						long usageCount = ElapsedTimeRecorder.totalCount(activeConnection.histogram());
						if (0==usageCount) {
			
								logger.warn("\n corrupt connection, retry");
								
        						///no notification needed. but we must clear this so the connection is rebuilt
							    activeConnection.clientClosedNotificationSent();
								activeConnection.close(); //close so next call we will get a fresh connection
								//old message will find new connection under old connection id!
			
						} else {
							//drop request and close the connection
							Pipe.skipNextFragment(requestPipe);
							//TODO: drop all while match??
							activeConnection.close();
						}
						blockedCycles = 0;
					}

					activeConnection = null;
					return false;
					
				} else {
					blockedCycles = 0;
				}
			} else {
				blockedCycles = 0;
			}
			
			//we do this later so the handshake logic above has an opportunity to 'timeout'
			if (activeConnection.isBusy() || !Pipe.hasRoomForWrite(output[activeConnection.requestPipeLineIdx()])) {
				return false;//must try again later when the server has responded.
			}
			return true;
			
			
		} else {
			return false;
		}
	}

	int blockedCycles;
	
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