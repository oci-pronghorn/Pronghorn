package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.channels.Selector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.util.PoolIdx;
import com.ociweb.pronghorn.util.ServiceObjectHolder;
import com.ociweb.pronghorn.util.ServiceObjectValidator;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class ClientConnectionManager extends SSLConnectionHolder implements ServiceObjectValidator<ClientConnection>{

	private final ServiceObjectHolder<ClientConnection> connections;
	private final TrieParser hostTrie;
	private final TrieParserReader hostTrieReader;
	private final byte[] guidWorkspace = new byte[6+512];
	private final PoolIdx responsePipeLinePool;
	private Selector selector;
	private static final Logger logger = LoggerFactory.getLogger(ClientConnectionManager.class);
		
	//TOOD: may keep internal pipe of "in flight" URLs to be returned with the results...
	
	public ClientConnectionManager(int connectionsInBits, int maxPartialResponses) { 
		connections = new ServiceObjectHolder<ClientConnection>(connectionsInBits, ClientConnection.class, this, false);
		hostTrie = new TrieParser(4096,4,false,false);
		hostTrieReader = new TrieParserReader();
		responsePipeLinePool = new PoolIdx(maxPartialResponses); //NOTE: maxPartialResponses should never be greater than response listener count		
	}
		
	public SSLConnection get(long hostId, int groupId) {
		ClientConnection response = connections.getValid(hostId);
		if (null == response) {
			releaseResponsePipeLineIdx(hostId);
		} else {
			connections.incUsageCount(hostId);
		}
		return response;
	}
	
	/**
	 * 
	 * This method is not thread safe. 
	 * 
	 * @param host
	 * @param port
	 * @param userId
	 * @return -1 if the host port and userId are not found
	 */
	public long lookup(CharSequence host, int port, int userId) {	
		//TODO: lookup by userID then by port then by host??? may be a better approach instead of guid 
		int len = ClientConnection.buildGUID(guidWorkspace, host, port, userId);		
		return TrieParserReader.query(hostTrieReader, hostTrie, guidWorkspace, 0, len, Integer.MAX_VALUE);
	}
	
	
	public long lookupInsertPosition() {
		return connections.lookupInsertPosition();
	}
	
	public int responsePipeLineIdx(long ccId) {
		return responsePipeLinePool.get(ccId);
	}
	
	
	public void releaseResponsePipeLineIdx(long ccId) {
		responsePipeLinePool.release(ccId);
	}
	
	public int resposePoolSize() {
		return responsePipeLinePool.length();
	}

	@Override
	public boolean isValid(ClientConnection connection) {
		return connection.isValid();
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

	public static long openConnection(ClientConnectionManager ccm, CharSequence host, int port, int userId, int pipeIdx) {
		long connectionId = ccm.lookup(host, port, userId);				                
		if (-1 == connectionId || null == ccm.get(connectionId, 0)) {
			
			connectionId = ccm.lookupInsertPosition();
			
			if (connectionId<0) {
				logger.warn("too many open connection, consider opening fewer for raising the limit of open connections above {}",ccm.connections.size());
				//do not open instead we should attempt to close this one to provide room.
				return connectionId;
			}
			
			try {
		    	//create new connection because one was not found or the old one was closed
				ClientConnection cc = new ClientConnection(host.toString(), port, userId, pipeIdx, connectionId);				                	
		    	
		    	while (!cc.isFinishConnect() ) {  //TODO: revisit
		    		Thread.yield();
		    	}
	
		    	cc.beginHandshake(ccm.selector());
		    	
		    	ccm.connections.setValue(connectionId, cc);
				
				//store under host and port this hostId
				ccm.hostTrie.setValue(cc.GUID(), connectionId);
	
			} catch (IOException ex) {
				logger.warn("handshake problems with new connection {}:{}",host,port,ex);
				
				connectionId = Long.MIN_VALUE;
			}				                	
		}
		return connectionId;
	}


	
	
}
