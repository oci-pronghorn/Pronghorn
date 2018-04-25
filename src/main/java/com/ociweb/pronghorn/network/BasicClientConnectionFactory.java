package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;

public class BasicClientConnectionFactory extends AbstractClientConnectionFactory {

	public final static BasicClientConnectionFactory instance = new BasicClientConnectionFactory();
		
	public ClientConnection newClientConnection(ClientCoordinator ccm, CharSequence host, int port,
			int sessionId, long connectionId, int pipeIdx, int hostId, int structureId)
			throws IOException {
		
		SSLEngine engine =  ccm.isTLS ?
		        ccm.engineFactory.createSSLEngine(host instanceof String ? (String)host : host.toString(), port)
		        :null;
		   
		ClientConnection con = null;
		try {        
			con =new ClientConnection(engine, host, hostId, port, sessionId, pipeIdx, 
					                  connectionId, structureId);
		} catch (IOException e) {
			//close socket if this is unable to open
			SocketChannel local = con.getSocketChannel();
			if (null!=local) {
				try {
				  local.close();
				} catch (Exception ex) {
					//ignore
				}
			}
			throw e;
		}
		return con;
	}
}
