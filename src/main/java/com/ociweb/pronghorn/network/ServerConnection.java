package com.ociweb.pronghorn.network;

import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;

public class ServerConnection extends SSLConnection {

	protected ServerConnection(SSLEngine engine, SocketChannel socketChannel, long id) {
		super(engine, socketChannel, id);

	}

}
