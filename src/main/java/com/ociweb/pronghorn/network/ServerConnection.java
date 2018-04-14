package com.ociweb.pronghorn.network;

import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;

public class ServerConnection extends BaseConnection {

	protected ServerConnection(SSLEngine engine, 
			                   SocketChannel socketChannel, 
			                   long id, ServerCoordinator coordinator) {
		super(engine, socketChannel, id,
			  coordinator.connectionDataElements(),
			  coordinator.connectionDataElementSize());
	}

}
