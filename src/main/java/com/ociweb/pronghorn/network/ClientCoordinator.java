package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.channels.Selector;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.PoolIdx;
import com.ociweb.pronghorn.util.ServiceObjectHolder;
import com.ociweb.pronghorn.util.ServiceObjectValidator;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class ClientCoordinator extends SSLConnectionHolder implements ServiceObjectValidator<ClientConnection>{

	private final ServiceObjectHolder<ClientConnection> connections;
	private final TrieParser hostTrie;
	private final TrieParserReader hostTrieReader;
	private final byte[] guidWorkspace = new byte[6+512];
	private final PoolIdx responsePipeLinePool;
	private Selector selector;
	private static final Logger logger = LoggerFactory.getLogger(ClientCoordinator.class);
	private PronghornStage firstStage;
	
	public final boolean isTLS;
	//TOOD: may keep internal pipe of "in flight" URLs to be returned with the results...
	
    public void shutdown() {
    	
    	if (null!=firstStage) {
    		firstStage.requestShutdown();
    		firstStage=null;
    	}
    	logger.info("Client pipe pool:\n {}",responsePipeLinePool);
    	    	
    }
    

	public void setStart(PronghornStage startStage) {
		this.firstStage = startStage;
	}
	
    
	public ClientCoordinator(int connectionsInBits, int maxPartialResponses) { 
		this(connectionsInBits,maxPartialResponses,true);
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
		
	public SSLConnection get(long hostId, int groupId) {
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
		//TODO: lookup by userID then by port then by host??? may be a better approach instead of guid 
		int len = ClientConnection.buildGUID(workspace, hostBack, hostPos, hostLen, hostMask, port, userId);	
		
		
		long result = TrieParserReader.query(reader, hostTrie, workspace, 0, len, Integer.MAX_VALUE);
		
//		if (result<0) {
//			String host = Appendables.appendUTF8(new StringBuilder(), hostBack, hostPos, hostLen, hostMask).toString();
//			System.err.println("lookup "+host+":"+port+"   user "+userId);
//		
//			System.err.println("GUID: "+Arrays.toString(Arrays.copyOfRange(guidWorkspace,0,len)));
//			System.err.println(hostTrie);
//			
//		}
		
		assert(0!=result) : "connection ids must be postive or negative if not found";
		return result;
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

	public static ClientConnection openConnection(ClientCoordinator ccm, byte[] hostBack, int hostPos, int hostLen, int hostMask,
			                                      int port, int userId, int pipeIdx, Pipe<NetPayloadSchema>[] handshakeBegin) {				
		
		long connectionId = ccm.lookup(hostBack,hostPos,hostLen,hostMask, port, userId);			
		
		return openConnection(ccm, hostBack, hostPos, hostLen, hostMask, port, userId, pipeIdx, handshakeBegin,	connectionId);
	}


	public static ClientConnection openConnection(ClientCoordinator ccm, byte[] hostBack, int hostPos, int hostLen,
			int hostMask, int port, int userId, int pipeIdx, Pipe<NetPayloadSchema>[] handshakeBegin,
			long connectionId) {
		
		ClientConnection cc = null;
		
		if (-1 == connectionId || null == (cc = (ClientConnection) ccm.connections.get(connectionId))) { //NOTE: using straight get since un finished connections may not be valid.
						
			//logger.warn("Unable to lookup connection");
			
			connectionId = ccm.lookupInsertPosition();
			
			if (connectionId<0 || pipeIdx<0) {
				
				logger.warn("too many open connection, consider opening fewer for raising the limit of open connections above {}",ccm.connections.size());
				//do not open instead we should attempt to close this one to provide room.
				return null;
			}
			
		
			String host = Appendables.appendUTF8(new StringBuilder(), hostBack, hostPos, hostLen, hostMask).toString(); //TODO: not GC free
			try {
				
		    	//create new connection because one was not found or the old one was closed
				cc = new ClientConnection(host, hostBack, hostPos, hostLen, hostMask, port, userId, pipeIdx, connectionId);
				
				//logger.debug("saving new client connectino for "+connectionId);
				
				ccm.connections.setValue(connectionId, cc);	
				ccm.hostTrie.setValue(cc.GUID(), 0, cc.GUIDLength(), Integer.MAX_VALUE, connectionId);			
				
			} catch (IOException ex) {
				logger.warn("handshake problems with new connection {}:{}",host,port,ex);				
				connectionId = Long.MIN_VALUE;
			}
							                	
		}
		
		if (null!=cc && !cc.isRegistered()) {
			try {
				if (!cc.isFinishConnect()) {
					cc = null;//try again later
				} else {
					cc.registerForUse(ccm.selector(), handshakeBegin, ccm.isTLS);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}		

		return cc;
	}

	
}
