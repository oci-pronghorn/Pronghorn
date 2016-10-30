package com.ociweb.pronghorn.network;

import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSLConnection {
	
	static final Logger log = LoggerFactory.getLogger(SSLConnection.class);

	protected final SSLEngine engine;
	protected final SocketChannel socketChannel;
	protected final long id;
	protected boolean isValid = true;

	protected int localRunningBytesProduced;
	

	protected boolean isDisconnecting = false;
	protected static boolean isShuttingDown =  false;
	
	protected SSLConnection(SSLEngine engine, SocketChannel socketChannel, long id ) {
		this.engine = engine;
		this.socketChannel = socketChannel;
		this.id = id;
	}
	
	
    //should only be closed by the socket writer logic or TLS handshake may be disrupted causing client to be untrusted.
	public boolean close() {
		if (isValid) {
			isValid = false;
			try {
					getSocketChannel().close();
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
		    	if (!isShuttingDown) {	//if we are shutting down this is not an error.                    	
		    		log.trace("This engine was forced to close inbound, without having received the proper SSL/TLS close notification message from the peer, due to end of stream.", e);
		    	}
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
	
	public SSLEngine getEngine() {
		return engine;
	}

	public SocketChannel getSocketChannel() {
		return socketChannel;
	}


	
}
