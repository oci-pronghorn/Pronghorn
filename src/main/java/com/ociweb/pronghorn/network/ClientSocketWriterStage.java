package com.ociweb.pronghorn.network;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ClientSocketWriterStage extends PronghornStage {
	
	private final ClientConnectionManager ccm;
	private final Pipe<NetPayloadSchema>[] input;
	private int shutCountDown;
	
	protected ClientSocketWriterStage(GraphManager graphManager, ClientConnectionManager ccm, Pipe<NetPayloadSchema>[] input) {
		super(graphManager, input, NONE);
		this.ccm = ccm;
		this.input = input;
		this.shutCountDown = input.length;
		
	}

	@Override
	public void run() {
		
		int i = input.length;
		while (--i>=0) {		
			while (PipeReader.tryReadFragment(input[i])) try {			
				
								
				if (NetPayloadSchema.MSG_ENCRYPTED_200 == PipeReader.getMsgIdx(input[i])) {
									
					ClientConnection cc = (ClientConnection)ccm.get(PipeReader.readLong(input[i], NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201), 0);
					
					if (null!=cc) {
						if (!cc.writeToSocketChannel(input[i])) { //TODO: this is a blocking write to be converted to non blocking soon.
							cc.close();//unable to write request
						}
					} else {
						//TODO: this in important case to test.
						//can not send this connection was lost
						continue;
					}
				} else {
					
					if (NetPayloadSchema.MSG_PLAIN_210 == PipeReader.getMsgIdx(input[i])) {
					
						ClientConnection cc = (ClientConnection)ccm.get(PipeReader.readLong(input[i], NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201), 0);
						
						if (null!=cc) {
							cc.close();//this location is free to be re-used and this connection can not be fetched again.
						}
					}
					
					//what about disconnect meesage
					
				//	assert(-1 == PipeReader.getMsgIdx(input[i])) : "Expected end of stream shutdown";
					if (--this.shutCountDown <= 0) {
						requestShutdown();
						return;
					}					
				}				
				
			} finally {
				PipeReader.releaseReadLock(input[i]);
			}
		
		}
		
	}

}
