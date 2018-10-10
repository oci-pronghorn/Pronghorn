package com.ociweb.pronghorn.network;

import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;

import com.ociweb.pronghorn.network.schema.ConnectionStateSchema;
import com.ociweb.pronghorn.pipe.Pipe;

public class ServerConnection extends BaseConnection {

	public final ServerConnectionStruct scs;
		
	protected ServerConnection(SSLEngine engine, SocketChannel socketChannel, 
			                   long id, ServerCoordinator coordinator) {
		
		super(engine, socketChannel, id);
		
		this.scs  = coordinator.connectionStruct();
		assert(coordinator.connectionStruct() != null) : "server side connections require struct";
				
		Pipe<ConnectionStateSchema> pipe = ConnectionStateSchema.instance.newPipe(
					this.scs.inFlightCount(), this.scs.inFlightPayloadSize()
				);
		pipe.initBuffers();
		Pipe.structRegistry(pipe, scs.registry);
		
		this.connectionDataWriter = new ConnDataWriterController(pipe);
		this.connectionDataReader = new ConnDataReaderController(pipe);	
		
	}
	
	protected ServerConnection(SSLEngine engine, SocketChannel socketChannel, long id,
							   ConnDataWriterController connectionData, ServerConnectionStruct scs) {
		super(engine, socketChannel, id);
		this.connectionDataWriter = connectionData;
		this.scs = scs;
	}
	
}
