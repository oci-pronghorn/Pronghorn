package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ClientSocketWriterStage extends PronghornStage {
	
	private static final Logger logger = LoggerFactory.getLogger(ClientSocketWriterStage.class);
	
	private final ClientCoordinator ccm;
	private final Pipe<NetPayloadSchema>[] input;
	private ByteBuffer[] buffers;
	private ClientConnection[] connections;
	
	private int shutCountDown;

	private long start;
	private long totalBytes=0;
	
	
	public static ClientSocketWriterStage newInstance(GraphManager graphManager, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] input) {
		return new ClientSocketWriterStage(graphManager, ccm, input);
	}
	
	public ClientSocketWriterStage(GraphManager graphManager, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] input) {
		super(graphManager, input, NONE);
		this.ccm = ccm;
		this.input = input;
		this.shutCountDown = input.length;
	}

	
	@Override
	public void startup() {
		int i = input.length;
		connections = new ClientConnection[i];
		buffers = new ByteBuffer[i];
		while (--i>=0) {
			buffers[i] = ByteBuffer.allocateDirect(input[i].maxAvgVarLen);
		}
		start = System.currentTimeMillis();		
	}
	
	@Override
	public void shutdown() {
		long duration = System.currentTimeMillis()-start;
		
		logger.info("Client Bytes Written: {} kb/sec {} ",totalBytes, (8*totalBytes)/duration);
		
	}
	
	@Override
	public void run() {
		
		boolean didWork;
		
		do {
			didWork = false;
						
			int i = input.length;
			while (--i>=0) {
								
				if (connections[i]!=null) {
					tryWrite(i);
				} else {
					Pipe<NetPayloadSchema> pipe = input[i];
					while (PipeReader.tryReadFragment(pipe)) try {			
	
						//System.err.println("data to read "+Pipe.contentRemaining(input[i])+"    "+PipeReader.getMsgIdx(input[i]));
														
						if (NetPayloadSchema.MSG_ENCRYPTED_200 == PipeReader.getMsgIdx(pipe)) {
											
							ClientConnection cc = (ClientConnection)ccm.get(PipeReader.readLong(pipe, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201), 0);
	
							if (null!=cc) {
								int payloadSize = PipeReader.readBytesLength(pipe,  NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
								
								//logger.trace("encrypted data of {} written to next stage for {}",payloadSize,cc);
								
								totalBytes += payloadSize;
								
								ByteBuffer[] writeHolder = PipeReader.wrappedUnstructuredLayoutBuffer(pipe, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
	
								//.info("E found data and put in buffer");
	
								//copy done here to avoid GC and memory allocation done by socketChannel
								buffers[i].clear();
								buffers[i].put(writeHolder[0]);
								buffers[i].put(writeHolder[1]);
								buffers[i].flip();	
								connections[i] = cc;
								
								tryWrite(i);

							} else {
								if (false) {
									logger.error("UUUUUUUUUUUUUUUUUUUu client lost connection and did not send data for {} ", PipeReader.readLong(pipe, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201));
								}
								
								//clean shutdown of this connections resources
								buffers[i].clear();
								connections[i]=null;
								continue;
							}
						} else if (NetPayloadSchema.MSG_PLAIN_210 == PipeReader.getMsgIdx(pipe)) {
							
						//	logger.info("plain looking at pipe {}  {} ",i,input[i]);
	
	 						    long workingTail = PipeReader.readLong(pipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206);
						    	if (SSLUtil.HANDSHAKE_POS != workingTail) {
		 						    ClientConnection cc = (ClientConnection)ccm.get(PipeReader.readLong(pipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201), 0);
									
									if (null!=cc) {
										int length = PipeReader.readBytesLength(pipe,  NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);

										totalBytes += length;
								
										ByteBuffer[] writeHolder = PipeReader.wrappedUnstructuredLayoutBuffer(pipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
	
										//logger.info("P found data and put in buffer");
							
										//copy done here to avoid GC and memory allocation done by socketChannel
										buffers[i].clear();
										buffers[i].put(writeHolder[0]);
										buffers[i].put(writeHolder[1]);
										buffers[i].flip();	
																				
										connections[i] = cc;
										
										tryWrite(i);
									} else {
										logger.error("GGGGGGGGGGGGGGGggggg client lost connection and did not send datafor {} ", PipeReader.readLong(pipe, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201));
										//TODO: this in important case to test.
										//can not send this connection was lost
										continue;
									}
						    	} else {
						    		logger.error("Hanshake not supported here, this message should not have arrived");
						    	}
						} else {
							
							//what about disconnect meesage
							
							assert(-1 == PipeReader.getMsgIdx(pipe)) : "Expected end of stream shutdown";
							if (--this.shutCountDown <= 0) {
								requestShutdown();
								return;
							}		
						}
								
						
					} finally {
						didWork = true;
						PipeReader.releaseReadLock(pipe);
					}			
				}
				
				
				
				
			}
		} while (didWork);
		
	}

	private void tryWrite(int i) {
		assert(buffers[i].hasRemaining()) : "please, do not call if there is nothing to write.";		
		try {
			connections[i].getSocketChannel().write(buffers[i]);
		} catch (IOException e) {
			logger.info("excption while writing to socket. ",e);
			connections[i].close();
		}
		if (!buffers[i].hasRemaining()) {
			connections[i]=null;
		}
	}

}
