package com.ociweb.pronghorn.network;

import java.io.IOException;

import javax.net.ssl.SSLEngine;

public class BasicClientConnectionFactory extends AbstractClientConnectionFactory {

	public final static BasicClientConnectionFactory instance = new BasicClientConnectionFactory();
		
	public ClientConnection newClientConnection(ClientCoordinator ccm, int port,
			int sessionId, long connectionId, int requestPipeIdx, int responsePipeIdx, int hostId, long timeoutNS, int structureId)
			throws IOException {
		
		SSLEngine engine =  ccm.isTLS ?
		        ccm.engineFactory.createSSLEngine(ClientCoordinator.registeredDomain(hostId), port)
		        :null;
		   
		return new ClientConnection(engine, hostId, port, sessionId, requestPipeIdx, responsePipeIdx, 
					                  connectionId, timeoutNS, structureId);

	}
}
