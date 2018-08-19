package com.ociweb.pronghorn.network;

import java.io.IOException;

public abstract class AbstractClientConnectionFactory {

	public abstract ClientConnection newClientConnection(ClientCoordinator ccm, CharSequence host, int port,
			int sessionId, long connectionId, int pipeIdx, int hostId, long timeoutNS, int structureId)
			throws IOException;
	
}
