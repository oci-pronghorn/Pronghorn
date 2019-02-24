package com.ociweb.pronghorn.network.http;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.schema.ClientHTTPRequestSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
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
	
	static final byte[] BYTES_SPACE = " ".getBytes();
	static final byte[] BYTES_SPACE_SLASH = " /".getBytes();
	
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
					//to optimize the ChannelSocketWriter we need to group work together
					//as a result we process all the data together on a pipe before moving to the next
					while (Pipe.hasContentToRead(input[i]) && buildClientRequest(input[i])) {
						hasWork = true;
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
	
			//note this is a fixed pipe choice! Since this may be a TLS connection we must
			//never move to another pipe.  The Session is used to ensure we stay in the same place.
			//as a result it is never efficient to have more output pipes than we have sessions
			//that behavior would result in un-used pipes.
			Pipe<NetPayloadSchema> target = output[activeConnection.sessionId%output.length];
			
			if (!Pipe.hasRoomForWrite(target) ) {
				return false;
			}
			
 			 int seq = activeConnection.getSequenceNo();

			 if (0 == seq && ccm.isTLS) {
				
				 long started = activeConnection.getLastUsedTime();
				 long duration = System.nanoTime()-started;
				 
				 ///////////////////////////////////////
				 //BIG HACK for TLS handshake, we must wait 400 ms BEFORE we start sending data
				 //This may be a problem on the server side no expecting data so quickly?
				 //This is a problem on the server we are fixing on the client by 
				 //waiting for the server to fully process the handshake,
				 //TODO: urgent, must fix server ServerSocketReader -> SSLEngineUnwrap
				 //This only happens on first TLS call
				 
				 if (duration < 400_000_000L) { //TODO: this needs urgent attention but the problem may be in the server..
					 return false;
				 }
	
			 }
			
			 activeConnection.setSequenceNo(++seq);
			
			final int msgIdx = Pipe.takeMsgIdx(requestPipe);			
		    //we have already checked for connection so now send the request

			final long now = System.nanoTime();
			activeConnection.setLastUsedTime(now);
	       	
			if (ClientHTTPRequestSchema.MSG_GET_200 == msgIdx) {
				HTTPClientUtil.publish(
						HTTPVerbDefaults.GET,
						requestPipe, activeConnection, target, now, stageId);				
			
			} else if (ClientHTTPRequestSchema.MSG_POST_201 == msgIdx) {
				HTTPClientUtil.publishWithPayload(
						HTTPVerbDefaults.POST,
						requestPipe, activeConnection, target, now, stageId);
			
			} else if (ClientHTTPRequestSchema.MSG_HEAD_202 == msgIdx) {
				HTTPClientUtil.publish(
						HTTPVerbDefaults.HEAD,
						requestPipe, activeConnection, target, now, stageId);
			
			} else if (ClientHTTPRequestSchema.MSG_DELETE_203 == msgIdx) {
				HTTPClientUtil.publish(
						HTTPVerbDefaults.DELETE,
						requestPipe, activeConnection, target, now, stageId);
							
			} else if (ClientHTTPRequestSchema.MSG_PUT_204 == msgIdx) {
				HTTPClientUtil.publishWithPayload(
						HTTPVerbDefaults.PUT,
						requestPipe, activeConnection, target, now, stageId);
							
			} else if (ClientHTTPRequestSchema.MSG_PATCH_205 == msgIdx) {
				HTTPClientUtil.publishWithPayload(
						HTTPVerbDefaults.PATCH,
						requestPipe, activeConnection, target, now, stageId);
			
			} else if (ClientHTTPRequestSchema.MSG_CLOSECONNECTION_104 == msgIdx) {
				HTTPClientUtil.cleanCloseConnection(requestPipe, activeConnection, target);
				
			} else {				
				throw new UnsupportedOperationException("Unexpected Message Idx:"+msgIdx);
			}
						
			Pipe.confirmLowLevelRead(requestPipe, Pipe.sizeOf(ClientHTTPRequestSchema.instance, msgIdx));
			Pipe.releaseReadLock(requestPipe);	
		
		}
		return didWork;
	}


	
	private ClientConnection activeConnection =  null;

	private ClientConnection lastHandShake = null;
	
	//has side effect of storing the active connection as a member so it need not be looked up again later.
	private boolean isConnectionReadyForUse(Pipe<ClientHTTPRequestSchema> requestPipe) {
		
		
		//all the message fragments hold these fields in the same positions.
		int sessionId = Pipe.peekInt(requestPipe,  1);
		assert(sessionId>0) : "session must be greater than zero found: "+sessionId;
		int port = Pipe.peekInt(requestPipe,  2);
		int hostId = Pipe.peekInt(requestPipe,  3);
		long connectionId  = Pipe.peekLong(requestPipe, 4); 			
		int targetResponsePipe = Pipe.peekInt(requestPipe, 6);
		
		if (targetResponsePipe<0) {
			throw new UnsupportedOperationException("bad target response value");
		}
		assert(targetResponsePipe>=0) : "bad target response value";
		
		if (connectionId == -1) {
			connectionId = ClientCoordinator.lookup(hostId, port, sessionId);
		}
		
 		if (connectionId>=0 && (null==activeConnection || activeConnection.id!=connectionId)) {
 			activeConnection = (ClientConnection) ccm.connectionObjForConnectionId(connectionId, false); 			
 		}

		 				
 		boolean singleHandShakes = false;
 		if (singleHandShakes) {
 			//limits wraps to only 1 at a time.
 			//does not appear to be the problem try unwraps.
 			//check previous and if it is not done shaking do not start this one.... return false
 			ClientConnection temp = lastHandShake; //hack test..
 			if (ccm.isTLS && null!=temp) {
 				if (temp.getEngine().getHandshakeStatus() != HandshakeStatus.NOT_HANDSHAKING ) {
 					return false;//do not start a new one until this one is done.
 				}
 			}
 		}
 		
 		if (null!=activeConnection  //do not check connectionId since it can change
 			&& (activeConnection.sessionId==sessionId)
 			&& (activeConnection.hostId == hostId) 	
 			&& (!activeConnection.isDisconnecting())
 			&& (!activeConnection.isClientClosedNotificationSent())
 			
 				) {
 			//logger.info("this is the same connection we just used so no need to look it up");
 		} else {
 		 			
 			assert(null!=ClientCoordinator.registeredDomain(Pipe.peekInt(requestPipe,  3))) : "bad hostId";
 			assert( Pipe.peekInt(requestPipe,  2)!=0) : "sessionId must not be zero, MsgId:"+Pipe.peekInt(requestPipe);		
			activeConnection = ClientCoordinator.openConnection(ccm, 
																hostId,  //hostId 3
																port,  //port 2
																sessionId,  //session 1
																targetResponsePipe,
																output, connectionId, ccf);
			

			if (ccm.isTLS) {
				lastHandShake = activeConnection;
			}
 		}
 		
		return connectionStateChecks(requestPipe);
	}

	
	private boolean connectionStateChecks(Pipe<ClientHTTPRequestSchema> requestPipe) {
		if (null != activeConnection) {

			assert(activeConnection.isFinishConnect());
			
			if (ccm.isTLS) {	
					
				if (null != activeConnection.getEngine()) {
				
					//If this connection needs to complete a handshake first then do that and do not send the request content yet.
					//ALL the conent will be held here until the connection and its handshake is complete
					//If this takes too long we can open a "new" connection and do the handshake again without loosing any data.
					HandshakeStatus handshakeStatus = activeConnection.getEngine().getHandshakeStatus();
					if (needsToShake(handshakeStatus)) {					
						
						
						if (++blockedCycles >= CYCLE_LIMIT_HANDSHAKE) { //are often 10_000 without error.
							//if this is new connection but it is not hand shaking as expected 
							//then we will drop the connection and try again.
							long usageCount = ElapsedTimeRecorder.totalCount(activeConnection.histogram());
							if (0==usageCount) {
				
									//logger.warn("\n corrupt connection, retry");
									
	        						///no notification needed. but we must clear this so the connection is rebuilt
								    activeConnection.clientClosedNotificationSent();
									activeConnection.close(); //close so next call we will get a fresh connection
									//old message will find new connection under old connection id!
									activeConnection = null;
				
							} else {
								//drop request and close the connection
								Pipe.skipNextFragment(requestPipe);
								
								//TODO: drop all while match??
								
								activeConnection.close();
								activeConnection = null;
							}
							blockedCycles = 0;
						}
	
						return false;
						
					} else {
						lastHandShake = null;
						blockedCycles = 0;
					}
				
				} else {
					return false;//waiting for engine
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

	private boolean needsToShake(HandshakeStatus handshakeStatus) {
		return HandshakeStatus.FINISHED!=handshakeStatus 
			&& HandshakeStatus.NOT_HANDSHAKING!=handshakeStatus;
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