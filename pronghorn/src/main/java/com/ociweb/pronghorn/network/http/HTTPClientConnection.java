package com.ociweb.pronghorn.network.http;

import java.io.IOException;

import javax.net.ssl.SSLEngine;

import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.TrieParser;

public class HTTPClientConnection extends ClientConnection {

	private final StructRegistry schema;
	public final TrieParser headerParser;
	
	public HTTPClientConnection(SSLEngine engine, 
			 int hostId, int port, int sessionId, int requestPipeIdx, int responsePipeIdx,
			 long conId, StructRegistry schema, long timeoutNS, int structId, TrieParser headerParser) throws IOException {
		super(engine, hostId, port,
		     sessionId, requestPipeIdx, responsePipeIdx,
		     conId, timeoutNS, structId);
		this.schema = schema;
		this.headerParser = headerParser;
		
		
	}
	
	public <T extends Object> T associatedFieldObject(long fieldToken) {
		return schema.getAssociatedObject(fieldToken);
	}

	public int totalSizeOfIndexes() {
		return schema.totalSizeOfIndexes(structureId);
	}

	public TrieParser headerParser() {
		return headerParser;
	}

}
