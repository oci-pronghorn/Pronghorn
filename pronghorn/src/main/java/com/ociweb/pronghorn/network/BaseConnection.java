package com.ociweb.pronghorn.network;

import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.ConnectionStateSchema;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.ChannelWriter;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.util.primitive.ByteArrayQueue;
import com.ociweb.pronghorn.util.primitive.LongDateTimeQueue;

public abstract class BaseConnection {
	
	static final Logger logger = LoggerFactory.getLogger(BaseConnection.class);

	private SSLEngine engine;
	private SocketChannel socketChannel;
	public final long id;//MUST be final and never change.
	
	protected boolean isValid = true;
	protected boolean isDisconnecting = false;
	protected int localRunningBytesProduced; //NOTE: could be a boolean
	
	private long lastNetworkBeginWait = 0;
	
	private int sequenceNo;
    private long lastUsedTimeNS = System.nanoTime();

    private LongDateTimeQueue startTimes = new LongDateTimeQueue(16);
    private ByteArrayQueue[] echos = new ByteArrayQueue[0];


	
	protected BaseConnection(SSLEngine engine, 
			                 SocketChannel socketChannel,
			                 long id
				) {
		
		this.engine = engine; //this is null for non TLS
		this.socketChannel = socketChannel;		
		this.id = id;

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
	
    //should only be closed by the socket writer logic or TLS handshake may be disrupted causing client to be untrusted.
	public boolean close() {
		//logger.info("closed connection {}",id);
		if (isValid) {
			isValid = false;
			try {
				 //this call to close will also de-register the selector key
				 socketChannel.close();
			} catch (Throwable e) {					
			}			
			return true;
		} 		
		return false;
	}

	public boolean isValid() {
		return isValid;
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

	
	public boolean isDisconnecting() {
		return isDisconnecting;
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
	
	public void setPoolReservation(int value) {
		assert(-1==poolReservation);
		poolReservation = value;
		assert(value>=0);

	}
	
	public int getPoolReservation() {
		return poolReservation;
	}
	
	public void clearPoolReservation() {
		poolReservation = -1;
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
	
	public void writeStartTime(long timeNS) {
		boolean ok = startTimes.tryEnqueue(timeNS);
		assert(ok);
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

    public void markDataHead() {
    	startTimes.markHead();
    	int x = echos.length;
    	while (--x>=0) {
    		echos[x].markHead();
    	}
    }
    
    public void resetDataHead() {
    	startTimes.resetHead();
    	int x = echos.length;
    	while (--x>=0) {
    		echos[x].resetHead();
    	}
    }
    

    public void markDataTail() {
    	startTimes.markTail();
    	int x = echos.length;
    	while (--x>=0) {
    		echos[x].markTail();
    	}
    }
    
    public void resetDataTail() {
    	startTimes.resetTail();
    	int x = echos.length;
    	while (--x>=0) {
    		echos[x].resetTail();
    	}
    }
    
}
