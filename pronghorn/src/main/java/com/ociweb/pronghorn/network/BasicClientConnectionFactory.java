package com.ociweb.pronghorn.network;

import java.io.IOException;

import javax.net.ssl.SSLEngine;

public class BasicClientConnectionFactory extends AbstractClientConnectionFactory {

	public final static BasicClientConnectionFactory instance = new BasicClientConnectionFactory();
		
	public ClientConnection newClientConnection(ClientCoordinator ccm, CharSequence host, int port,
			int sessionId, long connectionId, int pipeIdx, int hostId, long timeoutNS, int structureId)
			throws IOException {
		
		SSLEngine engine =  ccm.isTLS ?
		        ccm.engineFactory.createSSLEngine(host instanceof String ? (String)host : host.toString(), port)
		        :null;
		   
		return new ClientConnection(engine, host, hostId, port, sessionId, pipeIdx, 
					                  connectionId, timeoutNS, structureId);

	}
}
