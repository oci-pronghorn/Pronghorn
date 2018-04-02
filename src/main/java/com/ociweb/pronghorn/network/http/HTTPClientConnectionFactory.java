package com.ociweb.pronghorn.network.http;

import java.io.IOException;

import javax.net.ssl.SSLEngine;

import com.ociweb.pronghorn.network.AbstractClientConnectionFactory;
import com.ociweb.pronghorn.network.ClientConnection;
import com.ociweb.pronghorn.network.ClientCoordinator;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.struct.BStructSchema;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class HTTPClientConnectionFactory extends AbstractClientConnectionFactory{
	
	
	private final BStructSchema recordTypeData;

	private static final int initSize = 16;
	TrieParser[] headerParsers=new TrieParser[initSize];
	
	public HTTPClientConnectionFactory(BStructSchema recordTypeData) {
		this.recordTypeData = recordTypeData;
	}

	@Override
	public ClientConnection newClientConnection(ClientCoordinator ccm, 
											CharSequence host, int port,
			                                int sessionId, //unique value for this connection definition
											long connectionId, int pipeIdx, 
											int hostId, int structureId) throws IOException {
		
		SSLEngine engine =  ccm.isTLS ?
		        ccm.engineFactory.createSSLEngine(host instanceof String ? (String)host : host.toString(), port)
		        :null;
		        
	    if (sessionId>=headerParsers.length) {
	    	//grow both
	    	headerParsers = grow(headerParsers, sessionId*2);    	
	    }
	    if (null==headerParsers[sessionId]) {
	    	//build
	    	headerParsers[sessionId] = HTTPUtil.buildHeaderParser(
		    			recordTypeData, 
		    			structureId,
		    			HTTPHeaderDefaults.CONTENT_LENGTH,
		    			HTTPHeaderDefaults.TRANSFER_ENCODING,
		    			HTTPHeaderDefaults.CONTENT_TYPE
	    			);
	    
	    }		
				
		return new HTTPClientConnection(engine, host, hostId, port, sessionId, pipeIdx, 
				                    connectionId,
				                    recordTypeData, structureId,
				                    headerParsers[sessionId]);

		
	}

	private long[] grow(long[] source, int size) {
		long[] result = new long[size];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private TrieParser[] grow(TrieParser[] source, int size) {
		TrieParser[] result = new TrieParser[size];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

}
