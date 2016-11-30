package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.NetParseAckSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ClientSocketReaderStage extends PronghornStage {	
	
	private final ClientConnectionManager ccm;
	private final Pipe<NetPayloadSchema>[] output;
	private final Pipe<NetParseAckSchema>[] parseAck;
	private final static Logger logger = LoggerFactory.getLogger(ClientSocketReaderStage.class);

	private long start;
	private long totalBytes=0;
	private boolean isTLS;
	
	protected ClientSocketReaderStage(GraphManager graphManager, ClientConnectionManager ccm, Pipe<NetParseAckSchema>[] parseAck, Pipe<NetPayloadSchema>[] output, boolean isTLS) {
		super(graphManager, parseAck, output);
		this.ccm = ccm;
		this.output = output;
		this.parseAck = parseAck;
		this.isTLS = isTLS;
		
		//assert(ccm.resposePoolSize() == output.length);

		GraphManager.addNota(graphManager, GraphManager.PRODUCER, GraphManager.PRODUCER, this);
	}

	@Override
	public void startup() {
		start = System.currentTimeMillis();
	}
	
	@Override
	public void shutdown() {
		long duration = System.currentTimeMillis()-start;
		
		logger.info("Client Bytes Read: {} kb/sec {} ",totalBytes, (8*totalBytes)/duration);
		
	}

	int reportError = 50;
	
	@Override
	public void run() {
		try {
					
			Selector selector = ccm.selector();
			while (selector.selectNow()>0) {			
				
				Set<SelectionKey> keySet = selector.selectedKeys();
				Iterator<SelectionKey> keyIterator = keySet.iterator();
				while (keyIterator.hasNext()) {				
					SelectionKey selectionKey = keyIterator.next();
					
					if (!selectionKey.isValid()) {
						keyIterator.remove();
						System.err.println("key invalid");
						continue;
					}
					
					ClientConnection cc = (ClientConnection)selectionKey.attachment();
					
				    if (cc.isValid()) {
				    				    	
				    	//holds the pipe until we gather all the data and got the end of the parse.
				    	int pipeIdx = ccm.responsePipeLineIdx(cc.getId());//picks any open pipe to keep the decryption busy
				    	if (pipeIdx<0) {
				    		Thread.yield();
				    		consumeAck();
				    		pipeIdx = ccm.responsePipeLineIdx(cc.getId()); //try again.
//				    		if (pipeIdx<0 && --reportError>0) {
//				    			
//				    			logger.info("client reading data for connection {} and got pipeID of {} ",cc.getId(),pipeIdx); //TODO: same problem as server we starve out the handshakes...  
//				    							    			
//				    		}
				    		
				    		
				    	}
				    	
				    	
				    	if (pipeIdx>=0) {
				    		//was able to reserve a pipe run 
					    	Pipe<NetPayloadSchema> target = output[pipeIdx];
					    	
					    	if (PipeWriter.hasRoomForWrite(target)) {	    	
						    	
					    		ByteBuffer[] wrappedUnstructuredLayoutBufferOpen = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target,
					    				isTLS ?
					    				NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203 :
					    				NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204
					    				);
					    							    		
					    		int readCount = 0;
						    	long temp=0;
						    	do {						    		
									temp = cc.readfromSocketChannel(wrappedUnstructuredLayoutBufferOpen);
									
									//log.debug("reading {} from socket",temp);
				
						    		if (temp>0) {
						    			readCount+=temp;
						    		} else {
						    			if (temp<0 && readCount==0) {
						    				readCount=-1;
						    			}
						    			break;
						    		}
						     	} while (true);
						    	
						    	if (readCount>0) {
						    		totalBytes += readCount;
						    		
						    		//we read some data so send it		
						    		
						    		if (isTLS) {
							    		
							    		if (PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_ENCRYPTED_200)) {try {
							    			PipeWriter.writeLong(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201, cc.getId() );
							    			PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203, readCount);
							    		//    logger.info("from socket published          {} bytes for connection {} ",readCount,cc);
							    		} finally {
							    			PipeWriter.publishWrites(target);
							    		}} else {
							    			PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);
							    			logger.error("client is dropping incomming data. XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
							    			throw new RuntimeException("Internal error");
							    		}
							    		
						    		} else {
						    			
						    			if (PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_PLAIN_210)) {try {
							    			PipeWriter.writeLong(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, cc.getId() );
							    			PipeWriter.writeLong(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, Pipe.tailPosition(target) );							    			
							    			PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204, readCount);
							    		  //  logger.info("from socket published          {} bytes for connection {} ",readCount,cc);
							    		} finally {
							    			PipeWriter.publishWrites(target);
							    		}} else {
							    			PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);
							    			logger.error("client is dropping incomming data. XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
							    			throw new RuntimeException("Internal error");
							    		}
						    			
						    		}
						    		
						    		
						    	} else {
						    		//nothing to send so let go of byte buffer.
						    		PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);
						    		
						    	}
					    	} //else we try again
				    	} else {
				    		//not an error, just try again later.
				    	}
				    } else {
				    	System.err.println("not valid cc closed");
				    	//try again later
				    }
				    keyIterator.remove();//always remove we will be told again next time we call for select	   
				    consumeAck();
				}	
			}


			consumeAck();

			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
				
		
	}
	

	private void consumeAck() {
		
		int i = parseAck.length;
		while (--i>=0) {			
			Pipe<NetParseAckSchema> ack = parseAck[i];			
			while (Pipe.hasContentToRead(ack)) {
				int id = Pipe.takeMsgIdx(ack);
				if (id == NetParseAckSchema.MSG_PARSEACK_100) {
					long finishedConnectionId = Pipe.takeLong(ack);
					long pos = Pipe.takeLong(ack);
					///////////////////////////////////////////////////
	    			//if sent tail matches the current head then this pipe has nothing in flight and can be re-assigned
	    			if (Pipe.headPosition(output[ccm.responsePipeLineIdx(finishedConnectionId)]) == pos) {
	    				ccm.releaseResponsePipeLineIdx(finishedConnectionId);    				
	    			}
	    			Pipe.confirmLowLevelRead(ack, Pipe.sizeOf(NetParseAckSchema.instance, NetParseAckSchema.MSG_PARSEACK_100));
				} else {
					assert(-1 == id);
					Pipe.confirmLowLevelRead(ack, Pipe.EOF_SIZE);
				}
				Pipe.releaseReadLock(ack);
			}
			
		}
		
	}

}
