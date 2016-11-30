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
	
	private final ClientConnectionManager ccm;
	private final Pipe<NetPayloadSchema>[] input;
	private int shutCountDown;
	
	private ByteBuffer directBuffer;
	private long start;
	private long totalBytes=0;
	
	protected ClientSocketWriterStage(GraphManager graphManager, ClientConnectionManager ccm, Pipe<NetPayloadSchema>[] input) {
		super(graphManager, input, NONE);
		this.ccm = ccm;
		this.input = input;
		this.shutCountDown = input.length;

		
		
	}

	
	@Override
	public void startup() {
		
		directBuffer = ByteBuffer.allocateDirect(maxVarLength(input));
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
				while (PipeReader.tryReadFragment(input[i])) try {			
													
					if (NetPayloadSchema.MSG_ENCRYPTED_200 == PipeReader.getMsgIdx(input[i])) {
										
						ClientConnection cc = (ClientConnection)ccm.get(PipeReader.readLong(input[i], NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201), 0);

						if (null!=cc) {
							//logger.info("client write data for {}",cc);
							
							totalBytes += PipeReader.readBytesLength(input[i],  NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
							
							if (!cc.writeToSocketChannel(input[i], NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203, directBuffer)) { //TODO: this is a blocking write to be converted to non blocking soon.
								logger.error("AAAAAAAAAAAAAAAAA  client lost connection and did not send data for {} ", PipeReader.readLong(input[i], NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201));
								cc.close();//unable to write request
							}
						} else {
							logger.error("UUUUUUUUUUUUUUUUUUUu client lost connection and did not send data for {} ", PipeReader.readLong(input[i], NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201));
							//TODO: this in important case to test.
							//can not send this connection was lost
							continue;
						}
					} else if (NetPayloadSchema.MSG_PLAIN_210 == PipeReader.getMsgIdx(input[i])) {
						
							ClientConnection cc = (ClientConnection)ccm.get(PipeReader.readLong(input[i], NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201), 0);
							
							long workingTail = PipeReader.readLong(input[i], NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206);
							
							if (null!=cc) {
								//logger.info("client write data for {}",cc);
								
								totalBytes += PipeReader.readBytesLength(input[i],  NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
								
								if (!cc.writeToSocketChannel(input[i], NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204, directBuffer)) { //TODO: this is a blocking write to be converted to non blocking soon.
									logger.error("BBBBBBBBBBBBBBBBBBBB client lost connection and did not send datafor {} ", PipeReader.readLong(input[i], NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201));
									cc.close();//unable to write request
								}
							} else {
								logger.error("GGGGGGGGGGGGGGGggggg client lost connection and did not send datafor {} ", PipeReader.readLong(input[i], NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201));
								//TODO: this in important case to test.
								//can not send this connection was lost
								continue;
							}
					} else {
						
						//what about disconnect meesage
						
						assert(-1 == PipeReader.getMsgIdx(input[i])) : "Expected end of stream shutdown";
						if (--this.shutCountDown <= 0) {
							requestShutdown();
							return;
						}		
					}
							
					
				} finally {
					didWork = true;
					PipeReader.releaseReadLock(input[i]);
				}			
			}
		} while (didWork);
		
	}

}
