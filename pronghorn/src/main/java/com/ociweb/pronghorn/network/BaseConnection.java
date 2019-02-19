package com.ociweb.pronghorn.network;

import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.util.primitive.ByteArrayQueue;
import com.ociweb.pronghorn.util.primitive.LongDateTimeQueue;

public abstract class BaseConnection {
	
	static final Logger logger = LoggerFactory.getLogger(BaseConnection.class);

	private SSLEngine engine;
	private SocketChannel socketChannel;
	public final long id;//MUST be final and never change.
	
	public static final byte IS_NOT_VALID        = 1<<0;
	public static final byte IS_DISCONNECTING    = 1<<1;
	public static final byte IS_WRITE_REGISTERED = 1<<2;
		
	protected byte flags;
	
	protected int localRunningBytesProduced; //NOTE: could be a boolean
	
	private long lastNetworkBeginWait = 0;
	
	private int sequenceNo;
    private long lastUsedTimeNS = System.nanoTime();

    private LongDateTimeQueue startTimes; 
    private ByteArrayQueue[] echos;
	
	protected BaseConnection(SSLEngine engine, 
			                 SocketChannel socketChannel,
			                 long id,
			                 boolean initQueue
				) {
		
		this.engine = engine; //this is null for non TLS
		this.socketChannel = socketChannel;
		this.id = id;
		if (initQueue) {
			startTimes = new LongDateTimeQueue(13); //8K of data by default for start times
			echos = new ByteArrayQueue[0];
		}

	}
	
	/**
	 * Only called upon release. 
	 * This helps GC faster and eliminates the loop caused by this Connection held by 
	 * the Selector.
	 */
	public void decompose() {
		//new Exception("decompose connection").printStackTrace();//TODO: only called when we re-use slot...
		socketChannel = null;
		engine = null;		
	}
	
	public void setLastUsedTime(long timeNS) {
		lastUsedTimeNS = timeNS;
	}
	
	public long getLastUsedTime() {
		return lastUsedTimeNS;
	}
	
	public SSLEngine getEngine() {
			return engine;		
	}
	
	public String toString() {
		if (null==getEngine()) {
			return null==socketChannel? "Connection" : socketChannel.toString()+" id:"+id;
		} else {		
			return getEngine().getSession().toString()+" "
					+ ((null==socketChannel) ? "Connection" : (socketChannel.toString()+" id:"+id));
		}
	}
	
	
	public void setIsDisconecting() {
		flags |= IS_DISCONNECTING;
	}
	
	public void setIsNotValid() {
		flags |= IS_NOT_VALID;
	}
	
	public boolean isValid() {
		return 0 == (flags&IS_NOT_VALID);		
	}
	
	public boolean isDisconnecting() {
		return 1 == (flags&IS_DISCONNECTING);
	}
	
	public boolean isWriteRegistered() {
		return 1 == (flags&IS_WRITE_REGISTERED);
	}
	
	public void setIsWriteRegistered() {
		flags |=  IS_WRITE_REGISTERED;
	}
	
    //should only be closed by the socket writer logic or TLS handshake may be disrupted causing client to be untrusted.
	public boolean close() {
		//logger.info("closed connection {}",id);
		if (isValid()) {
			setIsNotValid();
			try {
				 //this call to close will also de-register the selector key
				 socketChannel.close();
			} catch (Throwable e) {					
			}			
			return true;
		} 		
		return false;
	}
	
	protected HandshakeStatus closeInboundCloseOutbound(SSLEngine engine) {
		
		try {
		    engine.closeInbound();
		} catch (SSLException e) {
		    boolean debug = false;
		    if (debug) {
		          	
		    		logger.trace("This engine was forced to close inbound, without having received the proper SSL/TLS close notification message from the peer, due to end of stream.", e);
		    
		    }
			
		}
		engine.closeOutbound();
		// After closeOutbound the engine will be set to WRAP state, in order to try to send a close message to the client.
		return engine.getHandshakeStatus();
	}

	
	
	public long getId() {
		return this.id;
	}

	public SocketChannel getSocketChannel() {
		return socketChannel;
	}


	public void clearWaitingForNetwork() {
		lastNetworkBeginWait=0;
	}


	public long durationWaitingForNetwork() {
		long now = System.nanoTime();
		if (0 == lastNetworkBeginWait) {
			lastNetworkBeginWait=now;
			return 0;
		} else {
			return now - lastNetworkBeginWait;
		}
	}

	public void setSequenceNo(int seq) {
		assert(seq>=sequenceNo) : "sequence numbers can only move forward.";
		sequenceNo = seq;
	}
	
	public int getSequenceNo() {
		//  log.info("getSequenceNo {} for chanel {}",sequenceNo,id);
		return sequenceNo;
	}

	private int poolReservation=-1;
	private int previousPoolReservation=-1;
	
	public void setPoolReservation(int value) {
		assert(-1==poolReservation);
		poolReservation = value;
		assert(value>=0);

	}
	
	public int getPoolReservation() {
		return poolReservation;
	}
	
	public void clearPoolReservation() {
		if (-1!=poolReservation) {
			previousPoolReservation = poolReservation;
		}
		poolReservation = -1;
	}
	
	public int getPreviousPoolReservation() {
		return previousPoolReservation;
	}

	@Override
	protected void finalize() throws Throwable {
		close();
	}
	
	//overwritten by leaf which has the headers
	public boolean hasHeadersToEcho() {
		return true;
	}
	
	public long readStartTime() {
		return startTimes.dequeue();
	}
	
	public int enqueueStartTime(long timeNS) {
		return startTimes.tryEnqueue(timeNS);	
	}
	public void publishStartTime(int headPos) {
		startTimes.publishHead(headPos);
	}
	
	
	public boolean hasDataRoom() {
		if (startTimes.hasRoom()) {
			int x = echos.length;
	    	while (--x>=0) {
	    		if (!echos[x].hasRoom()) {
	    			return false;
	    		};
	    	}
			return true;
		}		
		return false;
	}
    
}
