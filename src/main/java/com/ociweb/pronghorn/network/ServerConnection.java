package com.ociweb.pronghorn.network;

import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngine;

import com.ociweb.pronghorn.pipe.ChannelReaderController;
import com.ociweb.pronghorn.pipe.ChannelWriterController;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class ServerConnection extends BaseConnection {

	public final ServerConnectionStruct scs;
		
	protected ServerConnection(SSLEngine engine, SocketChannel socketChannel, 
			                   long id, ServerCoordinator coordinator) {
		
		super(engine, socketChannel, id);
		
		this.scs  = coordinator.connectionStruct();
		assert(coordinator.connectionStruct() != null) : "server side connections require struct";
				
		Pipe<RawDataSchema> pipe = RawDataSchema.instance.newPipe(
					this.scs.inFlightCount(), this.scs.inFlightPayloadSize()
				);
		pipe.initBuffers();
		Pipe.structRegistry(pipe, scs.registry);
		
		this.connectionDataWriter = new ChannelWriterController(pipe);
		this.connectionDataReader = new ChannelReaderController(pipe);	
		
	}
	
	protected ServerConnection(SSLEngine engine, SocketChannel socketChannel, long id,
							   ChannelWriterController connectionData, ServerConnectionStruct scs) {
		super(engine, socketChannel, id);
		this.connectionDataWriter = connectionData;
		this.scs = scs;
	}
	
}
