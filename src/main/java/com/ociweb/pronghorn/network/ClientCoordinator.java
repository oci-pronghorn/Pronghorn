package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.PronghornStageProcessor;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.PoolIdx;
import com.ociweb.pronghorn.util.ServiceObjectHolder;
import com.ociweb.pronghorn.util.ServiceObjectValidator;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class ClientCoordinator extends SSLConnectionHolder implements ServiceObjectValidator<ClientConnection>{

	private final ServiceObjectHolder<ClientConnection> connections;
	
	private final ReentrantReadWriteLock hostTrieLock = new ReentrantReadWriteLock();
	private final TrieParser hostTrie;
	
	public static boolean showHistogramResults = false;
	
	private final TrieParserReader hostTrieReader;
	private final byte[] guidWorkspace = new byte[6+512];
	private final PoolIdx responsePipeLinePool;
	private Selector selector;
	private static final Logger logger = LoggerFactory.getLogger(ClientCoordinator.class);
	private PronghornStage firstStage;
	
	public static boolean TEST_RECORDS = false;
	
	static {
		
	}

	
    public final static String expectedGet = "GET /groovySum.json HTTP/1.1\r\n"+
									  	     "Host: 127.0.0.1\r\n"+
										     "Connection: keep-alive\r\n"+
										     "\r\n";
    
	public final static String expectedOK = "HTTP/1.1 200 OK\r\n"+
											"Content-Type: application/json\r\n"+
											"Content-Length: 30\r\n"+
											"Connection: open\r\n"+
											"\r\n"+
											"{\"x\":9,\"y\":17,\"groovySum\":26}\n";
	
//	public final static String expectedOK = "HTTP/1.1 200 OK\r\n"+
//											"Server: nginx/1.10.0 (Ubuntu)\r\n"+
//											"Date: Mon, 16 Jan 2017 19:20:04 GMT\r\n"+
//											"Content-Type: application/json\r\n"+
//											"Content-Length: 30\r\n"+
//											"Last-Modified: Mon, 16 Jan 2017 16:50:59 GMT\r\n"+
//											"Connection: keep-alive\r\n"+
//											"ETag: \"587cf9f3-1e\"\r\n"+
//											"Accept-Ranges: bytes\r\n"+	
//											"\r\n"+
//											"{\"x\":9,\"y\":17,\"groovySum\":26}\n";
	
	
	
	public final boolean isTLS;

    private PronghornStageProcessor optionalStageProcessor;
    
	//TOOD: may keep internal pipe of "in flight" URLs to be returned with the results...
	
    public void shutdown() {
    	
    	if (null!=firstStage) {
    		firstStage.requestShutdown();
    		firstStage=null;
    	}
   // 	logger.info("Client pipe pool:\n {}",responsePipeLinePool);
    	    	
    	//logger.trace("Begin hisogram build");
    	
    	Histogram histRoundTrip = new Histogram(40_000_000_000L,0);
    	
    	int c = 5;
    	ClientConnection cc = connections.next();
    	do {
    		if (null!=cc) {
    			histRoundTrip.add(cc.histogram());
    		}
    			
    	} while (--c>=0 && null!=(cc = connections.next()));
    	
    	    	
    	if (showHistogramResults) {
    		//
    		histRoundTrip.outputPercentileDistribution(System.out, 1.0); 
    	}
    	
    }
    

	public void setStart(PronghornStage startStage) {
		this.firstStage = startStage;
	}
	
	
	public ClientCoordinator(int connectionsInBits, int maxPartialResponses, boolean isTLS) { 
		
		this.isTLS = isTLS;
		int maxUsers = 1<<connectionsInBits;
		int trieSize = 1024+(24*maxUsers); //TODO: this is a hack 
				
		
		connections = new ServiceObjectHolder<ClientConnection>(connectionsInBits, ClientConnection.class, this, false);
		hostTrie = new TrieParser(trieSize, 4, false, false); //TODO: ugent,  first boolean should be true but only false works,  this is a problem because it must be very large for large number of connecions !!!
		hostTrieReader = new TrieParserReader();
		responsePipeLinePool = new PoolIdx(maxPartialResponses); //NOTE: maxPartialResponses should never be greater than response listener count		
	}
		
	public SSLConnection get(long hostId) {
		ClientConnection response = connections.getValid(hostId);
		if (null == response) {			
			//logger.info("Release the pipe because the connection was discovered closed/missing. no valid connection found for "+hostId);
			
			releaseResponsePipeLineIdx(hostId);
			connections.resetUsageCount(hostId);
		} else {
			connections.incUsageCount(hostId);
		}
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
	/**
	 * 
	 * This method is not thread safe. 
	 * 
	 * @return -1 if the host port and userId are not found
	 */
	public long lookup(byte[] hostBack, int hostPos, int hostLen, int hostMask, int port, int userId) {	
		return lookup(hostBack,hostPos,hostLen,hostMask,port, userId, guidWorkspace, hostTrieReader);
	}
	
	public long lookup(byte[] hostBack, int hostPos, int hostLen, int hostMask, int port, int userId, byte[] workspace, TrieParserReader reader) {	
		//TODO: lookup by userID then by port then by host, may be a better approach instead of guid 
		int len = ClientConnection.buildGUID(workspace, hostBack, hostPos, hostLen, hostMask, port, userId);	
		
		hostTrieLock.readLock().lock();
		try {
			long result = TrieParserReader.query(reader, hostTrie, workspace, 0, len, Integer.MAX_VALUE);
			assert(0!=result) : "connection ids must be postive or negative if not found";
			return result;
		} finally {
			hostTrieLock.readLock().unlock();
		}
	}
	
	
	public long lookupInsertPosition() {
		return connections.lookupInsertPosition();
	}
	
	public int responsePipeLineIdx(long ccId) {
		return responsePipeLinePool.get(ccId);
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

	private static int findAPipeWithRoom(Pipe<NetPayloadSchema>[] output, int activeOutIdx) {
		int result = -1;
		//if we go around once and find nothing then stop looking
		int i = output.length;
		while (--i>=0) {
			//next idx		
			if (++activeOutIdx == output.length) {
				activeOutIdx = 0;
			}
			//does this one have room
			if (Pipe.hasRoomForWrite(output[activeOutIdx])) {
				result = activeOutIdx;
				break;
			}
		}
		return result;
	}
	
	
	public static ClientConnection openConnection(ClientCoordinator ccm, byte[] hostBack, int hostPos, int hostLen,
			int hostMask, int port, int userId, Pipe<NetPayloadSchema>[] outputs,
			long connectionId) {
								
		        ClientConnection cc = null;

				if (-1 == connectionId || 
					null == (cc = (ClientConnection) ccm.connections.get(connectionId))) { //NOTE: using direct lookup get since un finished connections may not be valid.
					//	logger.warn("Unable to lookup connection");					
					connectionId = ccm.lookupInsertPosition();
					
					int pipeIdx = findAPipeWithRoom(outputs, (int)Math.abs(connectionId%outputs.length));
					if (connectionId<0 || pipeIdx<0) {
						
						logger.warn("too many open connection, consider opening fewer for raising the limit of open connections above {}",ccm.connections.size());
						//do not open instead we should attempt to close this one to provide room.
						return null;
					}
					
				
					String host = Appendables.appendUTF8(new StringBuilder(), hostBack, hostPos, hostLen, hostMask).toString(); //TODO: not GC free
					try {
						
				    	//create new connection because one was not found or the old one was closed
						cc = new ClientConnection(host, hostBack, hostPos, hostLen, hostMask, port, userId, pipeIdx, connectionId, ccm.isTLS);
						ccm.connections.setValue(connectionId, cc);						
						ccm.hostTrieLock.writeLock().lock();
						
						try {
							ccm.hostTrie.setValue(cc.GUID(), 0, cc.GUIDLength(), Integer.MAX_VALUE, connectionId);
						} finally {
							ccm.hostTrieLock.writeLock().unlock();
						}
						
					} catch (IOException ex) {
						logger.warn("handshake problems with new connection {}:{}",host,port,ex);				
						connectionId = Long.MIN_VALUE;
						return null;
					}
									                	
				}
			
				if (cc.isRegistered()) {
					//logger.info("is registered {}",cc);
					return cc;
				}
				//not yet done so ensure it is marked.
				//cc.isFinishedConnection = false;
				//not registered
				return doRegister(ccm, outputs, cc);

	}


	private static ClientConnection doRegister(ClientCoordinator ccm, 
			                                   Pipe<NetPayloadSchema>[] handshakeBegin,
			                                   ClientConnection cc) {
		try {
			if (!cc.isFinishConnect()) {				
				//logger.info("unable to finish connect, must try again later {}",cc);				
				cc = null; //try again later
			} else {
				cc.registerForUse(ccm.selector(), handshakeBegin, ccm.isTLS);
				//logger.info("new connection established to {}",cc);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return cc;
	}

	
}
