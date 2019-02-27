package com.ociweb.pronghorn.network;

import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;

public class ServerConnection extends BaseConnection {

	public final ServerConnectionStruct scs;
		
	protected ServerConnection(SSLEngine engine, SocketChannel socketChannel, 
			                   long id, ServerCoordinator coordinator) {
		
		super(engine, socketChannel, id, true);
		
		this.scs  = coordinator.connectionStruct();
		assert(coordinator.connectionStruct() != null) : "server side connections require struct";
				
		
		//TODO: build queues. // this.scs.inFlightCount(), this.scs.inFlightPayloadSize()
		
				
	}
	
	@Override
	public boolean hasHeadersToEcho() {
		return scs.hasHeadersToEcho();
	}
	
}
