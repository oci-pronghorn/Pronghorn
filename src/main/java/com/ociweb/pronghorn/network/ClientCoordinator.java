package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.http.HTTPUtil;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.pipe.util.hash.LongLongHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.PronghornStageProcessor;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.PoolIdx;
import com.ociweb.pronghorn.util.ServiceObjectHolder;
import com.ociweb.pronghorn.util.ServiceObjectValidator;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.TrieParserReaderLocal;

public class ClientCoordinator extends SSLConnectionHolder implements ServiceObjectValidator<ClientConnection>{

	private final ServiceObjectHolder<ClientConnection> connections;
	
	//outstandingCallTime must be called on every object in the holder..
	//find the timeouts and mark them closed..
	//
	
	private final PoolIdx responsePipeLinePool;
	private Selector selector;
	private static final Logger logger = LoggerFactory.getLogger(ClientCoordinator.class);
	private PronghornStage firstStage;
	
	public static boolean TEST_RECORDS = false;
	
	//do not modify without sync on domainRegistry which is final
	public static int totalKnownDomains = 0;
	public static final TrieParser domainRegistry = new TrieParser(64, 2, false, false, true);

	private static final long EXPIRE_LIMIT_MS = 200;//if not used in MS then eligible to be closed.
	public static LongLongHashTable[] conTables = new LongLongHashTable[4];
	///////////////////////////////////////////////

	public static long busyCounter;//dirty count of occurences where client is waiting backed up.
	private final StructRegistry typeData;
    private PronghornStageProcessor optionalStageProcessor;
	public final int receiveBufferSize;
	//public long sentTime;
    
	//TOOD: may keep internal pipe of "in flight" URLs to be returned with the results...
	
    public void shutdown() {
    	
    	if (null!=firstStage) {
    		firstStage.requestShutdown();
    		firstStage=null;
    	}
      	
    }
    

	public void setStart(PronghornStage startStage) {
		this.firstStage = startStage;
	}
	
	
	public ClientCoordinator(int connectionsInBits, int maxPartialResponses, 
			                 TLSCertificates tlsCertificates, StructRegistry typeData) {
		super(tlsCertificates);
	
		/////////////////////////////////////////////////////////////////////////////////////
		//The trust manager MUST be established before any TLS connection work begins
		//If this is not done there can be race conditions as to which certs are trusted...
		if (isTLS) {
			engineFactory.initTLSService();
		}
		logger.trace("init of Client TLS called {}",isTLS);
		/////////////////////////////////////////////////////////////////////////////////////
		try {
			//get values we can not modify from the networking subsystem
			
			//the fake InetSocketAddress is set to ensure the RCVBUF is established as some value
			SocketChannel testChannel = SocketChannel.open();
			ClientConnection.initSocket(testChannel);
			receiveBufferSize = 1+testChannel.getOption(StandardSocketOptions.SO_RCVBUF);
			
			testChannel.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		this.typeData = typeData;
		this.connections = new ServiceObjectHolder<ClientConnection>(connectionsInBits, ClientConnection.class, this, false);

		this.responsePipeLinePool = new PoolIdx(maxPartialResponses,1); //NOTE: maxPartialResponses should never be greater than response listener count		
	}
		
	public void removeConnection(long id) {
		//logger.info("\n ****** remove this connection "+id,new Exception());
		
		releaseResponsePipeLineIdx(id);
		ClientConnection oldConnection = connections.remove(id);
		if (null != oldConnection) {
			//only decompose after removal.
			oldConnection.decompose();
		}
	}
	
	public BaseConnection connectionForSessionId(long id) {
		ClientConnection response = connections.get(id);
		
		if (null != response) {			
			if (response.isValid()) {
				connections.incUsageCount(id);
				return response;
			} else {
				//logger.info("connection was disconnected {}",id);
				//the connection has been disconnected
				response = null;
			}
		} else {
			//logger.info("got null lookup {}",id);
		}
		
		//logger.info("Release the pipe because the connection was discovered closed/missing. no valid connection found for "+hostId);
		releaseResponsePipeLineIdx(id);
		connections.resetUsageCount(id);
	
		return response;
	}
	
	public BaseConnection connectionForSessionId(long hostId, boolean alsoReturnDisconnected) {
		ClientConnection response = connections.get(hostId);
		
		if (null != response) {			
			if (response.isValid()) {
				connections.incUsageCount(hostId);
				return response;
			} else {
				//the connection has been disconnected
				if (!alsoReturnDisconnected) {
					response = null;
				}
			}
		}
		//logger.info("Release the pipe because the connection was discovered closed/missing. no valid connection found for "+hostId);
		releaseResponsePipeLineIdx(hostId);
		connections.resetUsageCount(hostId);
		
		return response;
	}
	
    public void setStageNotaProcessor(PronghornStageProcessor p) {
    	optionalStageProcessor = p;
    }
    public void processNota(GraphManager gm, PronghornStage stage) {
    	if (null != optionalStageProcessor) {
    		optionalStageProcessor.process(gm, stage);
    	}
    }

	
	public long lookupInsertPosition() {
			return connections.lookupInsertPosition();
	}
	
	public static int responsePipeLineIdx(ClientCoordinator that, long ccId) {
		return PoolIdx.get(that.responsePipeLinePool, ccId);
	}
		
	public int checkForResponsePipeLineIdx(long ccId) {
		return responsePipeLinePool.getIfReserved(ccId);
	}
	
	public void releaseResponsePipeLineIdx(long ccId) {
		responsePipeLinePool.release(ccId);
	}
	
	public int resposePoolSize() {
		return responsePipeLinePool.length();
	}

	@Override
	public boolean isValid(ClientConnection connection) {
		return null!=connection && connection.isValid();
	}


	@Override
	public void dispose(ClientConnection connection) {
		
		//we MUST open a new connection so we kill the oldest now.
		//logger.info("CLIENT SIDE BEGIN CONNECTION CLOSE");
		
		connection.beginDisconnect();
		
		//TODO: should we build a blocking write OR always keep some free.
		//connection.handShakeWrapIfNeeded(cc, target, buffer)
		
		connection.close();
		if (true) {
			throw new UnsupportedOperationException("Can not close old connection without finishing handshake.");
		}
	}

	/**
	 * loops over all valid connections and only returns null of there are no valid connections
	 * 
	 * import for shutdown which invalidates connections.
	 * 
	 * @return next valid open connection, or null of there are none.
	 */
	@Deprecated
	public ClientConnection nextValidConnection() {
		return connections.next();
	}
	
	public int maxClientConnections() {
		return connections.size();		
	}
	
	public ClientConnection getClientConnectionByPosition(int pos) {
		return connections.getByPosition(pos);
	}
	
	
	public Selector selector() {
		if (null==selector) {
			try {
				selector = Selector.open();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return selector;
	}

//	public final ClientConnection connectionId(CharSequence host, 
//			int port, int sessionId, 
//			Pipe<NetPayloadSchema>[] outputs, ClientConnection oldConnection) {
//		
//		if (host.length()==0) {
//			return null;
//		}
//		
//		if (null==oldConnection || ((!oldConnection.isValid()) && oldConnection.isFinishConnect() ) ) {
//			//only do reOpen if the previous one is finished connecting and its now invalid.
//			oldConnection = ClientCoordinator.openConnection(this, host, port,
//					sessionId, outputs,
//	                lookup(lookupHostId(host, new TrieParserReader(true)), port, sessionId));
//		}
//		
//		return oldConnection;
//		
//	}


	//we keep a single trie parser of all known domains, this is only
	//grown and mutated here. It must be synchronized since any thread can
	//create new instances of this object for connecting to a domain at any time.
	//this is almost always restricted to startup
	public synchronized static int registerDomain(CharSequence host) {

		int hostId = (int)TrieParserReader.query(new TrieParserReader(true), domainRegistry, host);
		if (-1==hostId) {
			hostId = totalKnownDomains++;
			domainRegistry.setUTF8Value(host, hostId);
			
			if (hostId==conTables.length) {
				LongLongHashTable[] bigger = new LongLongHashTable[conTables.length*2];
				System.arraycopy(conTables, 0, bigger, 0, conTables.length);
				conTables = bigger;				
			}
			conTables[hostId] = new LongLongHashTable(5); //This will grow as needed
						
		}

		return hostId;
	}
		
	public static long lookup(int hostId, int port, int sessionId) {
		int key = computePortSessionKey(port, sessionId);
		long result =  LongLongHashTable.getItem(conTables[hostId], key);
		
		if (0!=result) {
			return result;
		} else {
			if (LongLongHashTable.hasItem(conTables[hostId],key)) {
				return result;
			} else {
				return -1;
			}			
		}	
	}
	
	private static int computePortSessionKey(int port, int sessionId) {
		//the low 8 bits are part of session and port for both cases to minimize collisions.
		return (0xF&sessionId) | ((0xFFFF&port)<<4) | ((sessionId>>4)<<20);
	}
	
	
	public static int lookupHostId(CharSequence host, TrieParserReader reader) {
		assert(host.toString().trim().length()>0) : "ghost host";
		int result = (int)TrieParserReader.query(reader, domainRegistry, host);
		if (result>=0) {
			return result;
		} else {
			throw new UnsupportedOperationException("Before using domain at runtime you must call ClientCoordinator.registerDomain(\""+host+"\");");
		}
	}

	public static int lookupHostId(byte[] hostBytes) {
		return lookupHostId(hostBytes, 0, hostBytes.length, Integer.MAX_VALUE);
	}
	
	public static int lookupHostId(byte[] hostBytes, int pos, int length, int mask) {
		return (int)TrieParserReader.query(TrieParserReaderLocal.get(), domainRegistry,
					hostBytes, pos, length, mask);

	}



	private static int findAPipeWithRoom(Pipe<NetPayloadSchema>[] output, int seed) {
		int result = -1;
		//if we go around once and find nothing then stop looking
		int i = output.length;
		
		//when we reverse we want all these bits at the low end.
		int shiftForFlip = 33-Integer.highestOneBit(i);//33 because this is the length not the max value.
		
		//find the first one on the left most connection since we know it will share the same thread as the parent.
		int c = seed;
		while (--i>=0) {
			int activeIdx = Integer.reverse(c<<shiftForFlip)		
					        % output.length; //protect against non power of 2 outputs.
			
			if (Pipe.hasRoomForWrite(output[activeIdx])) { //  activeOutIdx])) {
				result = activeIdx;
				break;
			}
			c++;
			
		}
		return result;
	}
	
	private int clientConnectionsErrorCounter = 0;

	private ClientCoordinatorAbandonScanner abandonScanner = new ClientCoordinatorAbandonScanner();
	
	public static ClientConnection openConnection(ClientCoordinator ccm, 
			CharSequence host, int port, int sessionId, Pipe<NetPayloadSchema>[] outputs,
			long connectionId, AbstractClientConnectionFactory ccf) {
				
		        ClientConnection cc = null;

				if (-1 == connectionId || 
					null == (cc = (ClientConnection) ccm.connections.get(connectionId)) ||
					!cc.isValid()
						) { 
					//NOTE: using direct lookup get since un finished connections may not be valid.
										
					connectionId = ccm.lookupInsertPosition();
					
					if (connectionId<0) {
						long leastUsedId = (-connectionId);
						//take the least used connection but only
						//if it is not currently in use.
						ClientConnection tempCC = ccm.connections.getValid(leastUsedId);
						
						if ((tempCC==null) 
						//future feature	|| ((System.currentTimeMillis()System.currentTimeMillis()-tempCC.getLastUsedTime())>EXPIRE_LIMIT_MS)	
						   ) {							
							connectionId = leastUsedId;
							//logger.trace("client will reuse connection id {} ",leastUsedId);
						}
					}
					
					int pipeIdx = -1;
					if (connectionId<0
					    || (pipeIdx = findAPipeWithRoom(outputs, (int)Math.abs(connectionId%outputs.length)))<0) {
						return reportNoNewConnectionsAvail(ccm, connectionId);
					}

	
					//recycle from old one if it is found/given		        
					int hostId      = null!=cc? cc.hostId      : lookupHostId(host, TrieParserReaderLocal.get());	

					
					try {
					//	logger.info("\nnew client connection {}:{}",host,port);
				    	//create new connection because one was not found or the old one was closed
						cc = ccf.newClientConnection(ccm, host, port, sessionId, 
													connectionId, 
													pipeIdx, 
													hostId,
													structureId(sessionId, ccm.typeData));
						
					} catch (IOException ex) {
						logger.warn("\nUnable to open connection to {}:{}",host,port, ex);
						connectionId = Long.MIN_VALUE;
						return null;
					}
					
					ccm.connections.setValue(connectionId, cc);	
				
					long key = computePortSessionKey(cc.port, cc.sessionId);
					LongLongHashTable table = conTables[cc.hostId];					
					
					if (LongLongHashTable.isFull(table)) {
						conTables[cc.hostId] = table = LongLongHashTable.doubleClone(table);
					}
					LongLongHashTable.setItem(table, key, connectionId);
							                	
				}
				
				if (cc.isDisconnecting()) {
					return cc;
				}
				if (cc.isRegistered()) {
					//logger.info("is registered {}",cc);
					return cc;
				}
				//logger.info("doing register");
				
				//not yet done so ensure it is marked.
				//cc.isFinishedConnection = false;
				//not registered
				return doRegister(ccm, outputs, cc);

	}


	//we know that sessions are defined using a static counter so this is also safe. no collisions in JVM
	private static IntHashTable sessionStructIdTable = new IntHashTable(5);
	
	
	public static int structureId(int sessionId, StructRegistry typeData) {

		int result = IntHashTable.getItem(ClientCoordinator.sessionStructIdTable, sessionId);
		if (result!=0) {
			return result;
		} else {
			//was zero so find if its missing
			if (IntHashTable.hasItem(ClientCoordinator.sessionStructIdTable, sessionId)) {
				return result; //this zero is valid
			} else {
				//need to add new item				
				int newStructId = HTTPUtil.newHTTPStruct(typeData);
				if (!IntHashTable.setItem(ClientCoordinator.sessionStructIdTable, sessionId, newStructId)) {
					ClientCoordinator.sessionStructIdTable = IntHashTable.doubleSize(ClientCoordinator.sessionStructIdTable);
					if (!IntHashTable.setItem(ClientCoordinator.sessionStructIdTable, sessionId, newStructId)) {
						logger.warn("internal error, unable to store new struct id for reuse.");
					}
				}				
				return newStructId;
			}
		}
	}


	private static ClientConnection reportNoNewConnectionsAvail(ClientCoordinator ccm, long connectionId) {
		if (Integer.numberOfLeadingZeros(ccm.clientConnectionsErrorCounter)
			!=	Integer.numberOfLeadingZeros(++ccm.clientConnectionsErrorCounter)
				) {
			
			if (connectionId<0) {
				logger.warn("No ConnectionId Available, Too many open connections client side, consider opening fewer for raising the limit of open connections above {}",ccm.connections.size());								
			} else {
				logger.warn("No Free Data Pipes Available, Too many open connections client side, consider opening fewer for raising the limit of open connections above {}",ccm.connections.size());
			}							
			
		}
		
		
		//do not open instead we should attempt to close this one to provide room.
		return null;
	}



	private static ClientConnection doRegister(ClientCoordinator ccm,
			                                   Pipe<NetPayloadSchema>[] handshakeBegin,
			                                   ClientConnection cc) {
		
		//logger.info("\n ^^^ doRegister {}",cc.id);
		try {
			if (!cc.isFinishConnect()) {				
				//logger.info("\n ^^^^ unable to finish connect, must try again later {}",cc);	
				
				cc = null; //try again later
			} else {
				cc.registerForUse(ccm.selector(), handshakeBegin, ccm.isTLS);
				//logger.info("\n ^^^^ new connection established to {}",cc);
				
				BaseConnection con = ccm.connectionForSessionId(cc.id);
				assert(con==cc) : "unable to lookup connection";
				
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return cc;
	}


	public ClientConnection scanForAbandonedConnection() {
		abandonScanner.reset();
		connections.visitValid(abandonScanner);
		return abandonScanner.leadingCandidate();
	}


	
}
