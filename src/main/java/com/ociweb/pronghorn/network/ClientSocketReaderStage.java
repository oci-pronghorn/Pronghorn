package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ClientSocketReaderStage extends PronghornStage {	
	
	private final ClientCoordinator ccm;
	private final Pipe<NetPayloadSchema>[] output;
	private final Pipe<ReleaseSchema>[] releasePipes;
	private final static Logger logger = LoggerFactory.getLogger(ClientSocketReaderStage.class);

	private long start;
	private long totalBytes=0;
	private boolean isTLS;
	private final static int KNOWN_BLOCK_ENDING = -1;
	private Set<SelectionKey> keySet;
	
	public ClientSocketReaderStage(GraphManager graphManager, ClientCoordinator ccm, Pipe<ReleaseSchema>[] parseAck, Pipe<NetPayloadSchema>[] output, boolean isTLS) {
		super(graphManager, parseAck, output);
		this.ccm = ccm;
		this.output = output;
		this.releasePipes = parseAck;
		this.isTLS = isTLS;

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

	int maxWarningCount = 10;
	
	
	@Override
	public void run() {

			int didWork=1;
			
			do {	
				didWork--;
				
				boolean sentData = false;
				

					ClientConnection cc;
					
					//making this an if causes hang on large files!  TODO: urgent fix.
					while (null!=(cc  = ccm.nextValidConnection() )) { //TODO: do not use this global counter it may allow for skiping that we need to clenup??


					    if (cc.isValid()) {
					    		
					    	
					    	//process handshake before reserving one of the pipes
					    	if (isTLS) {
					    		
					    		HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();
					    		//logger.info("has data for {} {} {}",cc,cc.isValid(),handshakeStatus);
					    		
					    		
					    		 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
						                Runnable task;//TODO: there is anopporuntity to have this done by a different stage in the future.
						                while ((task = cc.getEngine().getDelegatedTask()) != null) {
						                	task.run();
						                }
						                handshakeStatus = cc.getEngine().getHandshakeStatus();
								 } else if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
									 consumeRelease();
									 assert(-1 == ccm.checkForResponsePipeLineIdx(cc.getId())) : "should have already been relased";								 

//									 if (--maxWarningCount>0) {//this should not be a common error but needs to be here to promote good configurations
//						    				logger.warn("waiting on wrap, need more pipes????");
//						    			}
									 continue;//one of the other pipes can do work
								 }	
					    				    		 
					    		 
						    	 if (false && handshakeStatus!=HandshakeStatus.FINISHED && handshakeStatus!=HandshakeStatus.NOT_HANDSHAKING) {
						    			//TOOD: this has been triggered
						    			assert(-1 == ccm.checkForResponsePipeLineIdx(cc.getId())) : "expected NO reserved pipe for "+cc.id+" with handshake of "+handshakeStatus;
						    	 }
					    	}

					    	
					    	//holds the pipe until we gather all the data and got the end of the parse.
					    	consumeRelease();
					    	int pipeIdx = ccm.responsePipeLineIdx(cc.getId());//picks any open pipe to keep the decryption busy
					    	if (pipeIdx<0) {				    	
					    		consumeRelease();
					    		pipeIdx = ccm.responsePipeLineIdx(cc.getId()); //try again.
					    		if (pipeIdx<0) {
//					    			if (--maxWarningCount>0) {//this should not be a common error but needs to be here to promote good configurations
//					    				logger.warn("bump up maxPartialResponsesClient count, performance is slowed due to waiting for available input pipe on client");
//					    			}
					    			continue;//we can not allocate a new pipe but on of the other previously assigned pipes may be empty so continue here.
					    		}				    		
					    	}
					    	
					    	//logger.trace("pipe idx {} ",pipeIdx);
					    	
					    	if (pipeIdx>=0) {
					    		//was able to reserve a pipe run 
						    	Pipe<NetPayloadSchema> target = output[pipeIdx];
						    	if (PipeWriter.hasRoomForWrite(target)) {	    	
							    	
						    		ByteBuffer[] wrappedUnstructuredLayoutBufferOpen = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target,
																				    				isTLS ?
																				    				NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203 :
																				    				NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204
																				    				);
						    			
						    		//TODO: add assert that target bufer is larger than socket buffer.
						    		//TODO: warning note cast to int.
						    		int readCount=-1; 
						    		try {
						    			SocketChannel socketChannel = (SocketChannel)cc.getSocketChannel();//selectionKey.channel();
						    			readCount = (int)socketChannel.read(wrappedUnstructuredLayoutBufferOpen, 0, wrappedUnstructuredLayoutBufferOpen.length);
						    		} catch (IOException ioex) {
						    			logger.info("unable to read socket, may not be an error. ",ioex);
						    			//will continue with readCount of -1;
						    		}
							    	boolean fullBuffer = wrappedUnstructuredLayoutBufferOpen[0].remaining()==0 && wrappedUnstructuredLayoutBufferOpen[1].remaining()==0;
							    	
							    	//logger.trace("client reading {} for id {} fullbuffer {}",readCount,cc.getId(),fullBuffer);
							    	
							    	if (readCount>0) {
							    		sentData =  true;							    		
							    		totalBytes += readCount;						    		
							    		//we read some data so send it		
							    	
							    		//logger.info("totalbytes consumed by client {} ",totalBytes);
							    		
							    	//	logger.info("client reading {} for id {}",readCount,cc.getId());
							    		
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
								    			PipeWriter.writeLong(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, KNOWN_BLOCK_ENDING);						    			
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
							    	
						    	} else {
						    		
						    		 //if (--maxWarningCount>0) {//this should not be a common error but needs to be here to promote good configurations
						    		//		logger.warn("we have no room on the pipe to write to {} {}.",cc.getId(),target);
						    		//	}
						    		
						    	}
					    	} else {
					    		//not an error, just try again later.
					    		
								 if (--maxWarningCount>0) {//this should not be a common error but needs to be here to promote good configurations
					    				logger.warn("odd we should not be here for this test.");
					    			}
					    	}
					    } else {
					    	logger.info("connection was no longer valid so this value has been removed.");
					    //	keyIterator.remove();
					    }
					    
					}	
					
					if (sentData) {
						didWork++;
					}
			
			} while(didWork>0);

				
		boolean debug = false;
		if (debug) {
			if (lastTotalBytes!=totalBytes) {
				System.err.println("Client reader total bytes :"+totalBytes);
				lastTotalBytes =totalBytes;
			}
		}
	}
	
	long lastTotalBytes = 0;

	private boolean hasData(Selector selector) throws IOException {
		
		if (null==keySet || keySet.isEmpty()) {
			int x = selector.selectNow();
			//logger.trace("new select of {} values",x);
			assert(selector.isOpen());
			keySet = selector.selectedKeys();
			
		} //else {
		//	logger.trace("size {}", keySet.size());
		//}
		
		boolean isEmpty = keySet.isEmpty();
		
		return !isEmpty;
	}
	
   //must be called often to keep empty.
	private void consumeRelease() {
		
		int i = releasePipes.length;
		while (--i>=0) {			
			Pipe<ReleaseSchema> ack = releasePipes[i];			
			while (Pipe.hasContentToRead(ack)) {
				int id = Pipe.takeMsgIdx(ack);
				if (id == ReleaseSchema.MSG_RELEASE_100) {
					long finishedConnectionId = Pipe.takeLong(ack);
					long pos = Pipe.takeLong(ack);

					///////////////////////////////////////////////////
	    			//if sent tail matches the current head then this pipe has nothing in flight and can be re-assigned
	    			int pipeIdx = ccm.checkForResponsePipeLineIdx(finishedConnectionId);
					if (pipeIdx>=0 && Pipe.headPosition(output[pipeIdx]) == pos) {
	    				ccm.releaseResponsePipeLineIdx(finishedConnectionId);
	    				
	    				//TODO: upon release must prioritize the re-open.
	    				//logger.info("did release for {}",finishedConnectionId);
	    				
	    			} else {
	    				if (pipeIdx>=0) {
	    					if (pos>Pipe.headPosition(output[pipeIdx])) {
	    						logger.info("GGGGGGGGGGGGGGGGGGGGGGGGGGgg unable to release pipe {} pos {} expected {}",pipeIdx,pos,Pipe.headPosition(output[pipeIdx]));
	    						//	System.exit(-1);
	    					} else {
	    						HandshakeStatus handshakeStatus = ccm.get(finishedConnectionId, 0).engine.getHandshakeStatus();
					    		if (handshakeStatus!=HandshakeStatus.FINISHED && handshakeStatus!=HandshakeStatus.NOT_HANDSHAKING) {
					    			//TOOD: this has been triggered
					    			assert(-1 == ccm.checkForResponsePipeLineIdx(finishedConnectionId)) : "expected no reserved pipe for "+finishedConnectionId+" with handshake of "+handshakeStatus;
					    		}
	    						
					    		//logger.info("no client side release {} ",releasePipes[i]);
	    						
	    						//this is the expected case where we have already put data on this pipe so it can not be released.
	    					}
	    				}
	    			}
	    			
	    			Pipe.confirmLowLevelRead(ack, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASE_100));
				} else {
					assert(-1 == id);
					Pipe.confirmLowLevelRead(ack, Pipe.EOF_SIZE);
				}
				Pipe.releaseReadLock(ack);
			}
			
		}
		
	}

}
