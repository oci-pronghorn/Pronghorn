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
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ClientSocketReaderStage extends PronghornStage {	
	
	private final ClientConnectionManager ccm;
	private final Pipe<NetPayloadSchema>[] output2;
	private final Pipe<NetParseAckSchema> parseAck;
	private Logger log = LoggerFactory.getLogger(ClientSocketReaderStage.class);

	
	protected ClientSocketReaderStage(GraphManager graphManager, ClientConnectionManager ccm, Pipe<NetParseAckSchema> parseAck, Pipe<NetPayloadSchema>[] output) {
		super(graphManager, parseAck, output);
		this.ccm = ccm;
		this.output2 = output;
		this.parseAck = parseAck;
		
		//assert(ccm.resposePoolSize() == output.length);

		GraphManager.addNota(graphManager, GraphManager.PRODUCER, GraphManager.PRODUCER, this);
	}

	@Override
	public void startup() {
		//this thread must no delay to take things out of the buffer
		//Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
	}

	@Override
	public void run() {

		try {
					
			Selector selector = ccm.selector();
			if (selector.selectNow()>0) {			
				
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
				    				    	
				    	int pipeIdx = ccm.responsePipeLineIdx(cc.getId());
				    	if (pipeIdx>=0) {
				    		//was able to reserve a pipe run 
					    	Pipe<NetPayloadSchema> target = output2[pipeIdx];
					    	
//					    	while (!PipeWriter.hasRoomForWrite(target)) {
//					    		Thread.yield();
//					    	}
					    	
					    	if (PipeWriter.hasRoomForWrite(target)) {	    	
						    	
					    		ByteBuffer[] wrappedUnstructuredLayoutBufferOpen = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target,NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
					    							    		
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
						    		//log.debug("read count from socket {} vs {} ",readCount,  wrappedUnstructuredLayoutBufferOpen.position()-p);
						    		//we read some data so send it			    					    		
						    		if (PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_ENCRYPTED_200)) try {
						    			PipeWriter.writeLong(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201, cc.getId() );
						    			PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203, readCount);
						    		    //log.info("from socket published          {} bytes ",readCount);
						    		} finally {
						    			PipeWriter.publishWrites(target);
						    		} else {
						    			throw new RuntimeException("Internal error");
						    		}
						    		
						    	} else {
						    		//nothing to send so let go of byte buffer.
						    		PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);
						    		
						    	}
					    	} //else we try again
				    	}
				    } else {
				    	System.err.println("not valid cc closed");
				    	//try again later
				    }
				    keyIterator.remove();//always remove we will be told again next time we call for select	    	
				}	
			}

			//ack can only come back after we sent some data and therefore the key is on the set.
			//for each ack find the matching key and remove it.
			
			//TODO keep with the reader, we stop reading when we get this signal.
			while (PipeReader.tryReadFragment(parseAck)) try {

				if (PipeReader.getMsgIdx(parseAck)==NetParseAckSchema.MSG_PARSEACK_100) {				
									
					long finishedConnectionId = PipeReader.readLong(parseAck, NetParseAckSchema.MSG_PARSEACK_100_FIELD_CONNECTIONID_1);
					
					ClientConnection clientConnection = (ClientConnection)ccm.get(finishedConnectionId, 0);
					//only remove after all the in flight messages are consumed
					if ((null==clientConnection) || (clientConnection.incResponsesReceived())) {
						assert((null==clientConnection) || (clientConnection.getId()==finishedConnectionId));
						//connection may still be open but we will release the pipeline
						ccm.releaseResponsePipeLineIdx(finishedConnectionId);
					}
				}
				
			} finally {
				PipeReader.releaseReadLock(parseAck);
			}

			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		
		
	}

}
