package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

public class ClientSocketReaderStage extends PronghornStage {	
	
	private static final int SIZE_OF_PLAIN = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210);
	private final ClientCoordinator coordinator;
	private final Pipe<NetPayloadSchema>[] output;
	private final Pipe<ReleaseSchema>[] releasePipes;
	private final static Logger logger = LoggerFactory.getLogger(ClientSocketReaderStage.class);

	public static final boolean showResponse = false;
	
	private long start;
	private long totalBytes=0;

	private final static int KNOWN_BLOCK_ENDING = -1;

	private final int maxClients;

	
	public ClientSocketReaderStage(GraphManager graphManager, ClientCoordinator coordinator, Pipe<ReleaseSchema>[] parseAck, Pipe<NetPayloadSchema>[] output) {
		super(graphManager, parseAck, output);
		this.coordinator = coordinator;
		this.output = output;
		this.releasePipes = parseAck;

		this.maxClients = coordinator.maxClientConnections();		
		
		coordinator.setStart(this);
		
		//this resolves the problem of detecting this loop by the scripted fixed scheduler.
		GraphManager.addNota(graphManager, GraphManager.PRODUCER, GraphManager.PRODUCER, this);
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lavenderblush", this);	
	}
	
	@Override
	public void startup() {
		
//		try {
//			selector = Selector.open();
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
				
		
		
		start = System.currentTimeMillis();

		
	}
	
	@Override
	public void shutdown() {
		long duration = System.currentTimeMillis()-start;
		if (duration>0) {
			logger.trace("Client Bytes Read: {} kb/sec {} ",totalBytes, (8*totalBytes)/duration);
		}
	}

	int maxWarningCount = 10;
	
	@Override
	public void run() { //TODO: this method is the new hot spot in the profiler.

		    consumeRelease();
		    
			boolean didWork;
			
			do {	
				    didWork = false;
					ClientConnection cc;
					
					int cpos = maxClients;
					while (--cpos>=0) { 
						
						//TODO: this is slow because we are NOT using the e-poll mechanism and we check every connection for data
						//      this results in many zero reads...
						cc = coordinator.getClientConnectionByPosition(cpos);
						
						
					    if (cc!=null) {
					    	
					    	//selector notes...
//					    	Selector selector = Selector.open();
//					    	cc.getSocketChannel().register(selector, SelectionKey.OP_READ);
//					    	
//				        	/////////////
//				        	//CAUTION - select now clears pevious count and only returns the additional I/O opeation counts which have become avail since the last time SelectNow was called
//				        	////////////        	
//				            pendingSelections = selector.selectNow();
				            
				            
					    	
					    	//process handshake before reserving one of the pipes
					    	if (coordinator.isTLS) {
					    		
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
									 
								//	 if (--maxWarningCount>0) {//this should not be a common error but needs to be here to promote good configurations
						    	//			logger.warn("waiting on wrap, need more pipes???? {}",cc.id);
						    	//		}
									 continue;//one of the other pipes can do work
								 }	
					    				    		 
					    		 
					    	}

					    	
					    	//holds the pipe until we gather all the data and got the end of the parse.
					    	int pipeIdx = ClientCoordinator.responsePipeLineIdx(coordinator, cc.getId());//picks any open pipe to keep the system busy
					    	if (pipeIdx>=0) {
					    	} else {	    	
					    		consumeRelease();
					    		pipeIdx = ClientCoordinator.responsePipeLineIdx(coordinator, cc.getId()); //try again.
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
	
						    	if (Pipe.hasRoomForWrite(target)) {
						        		
						    		//TODO: use epoll to detect new data??				
						    		
						    		
						    		//these buffers are only big enought to accept 1 target.maxAvgVarLen
						    		ByteBuffer[] wrappedUnstructuredLayoutBufferOpen = Pipe.wrappedWritingBuffers(target);

						    		//TODO: add assert that target bufer is larger than socket buffer.
						    		//TODO: warning note cast to int.
						    		int readCount=-1; 
						    		try {					    									    			
						    			readCount = (int)((SocketChannel)cc.getSocketChannel()).read(wrappedUnstructuredLayoutBufferOpen, 0, wrappedUnstructuredLayoutBufferOpen.length);
						    			
						    		} catch (IOException ioex) {
						    			readCount = -1;
						    			//logger.info("unable to read socket, may not be an error. ",ioex);
						    			//will continue with readCount of -1;
						    		}
						    
							    	
						    		if (readCount>0) {
						    			didWork = true;
							    		totalBytes += readCount;						    		
							    		//we read some data so send it		
							    	
							    		logger.trace("totalbytes consumed by client {} TLS {} ",totalBytes, coordinator.isTLS);
							    		
							    	//	logger.info("client reading {} for id {}",readCount,cc.getId());
							    		
							    		if (coordinator.isTLS) {
							    			assert(Pipe.hasRoomForWrite(target)) : "checked earlier should not fail";
							    			
							    			int size = Pipe.addMsgIdx(target, NetPayloadSchema.MSG_ENCRYPTED_200);
							    			Pipe.addLongValue(cc.getId(), target);
							    			Pipe.addLongValue(System.nanoTime(), target);
							    			
							    			int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(target);
							    			Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int)readCount, target);
							    			
							    			Pipe.confirmLowLevelWrite(target, size);
							    			Pipe.publishWrites(target);
							    										    		
							    		} else {
							    			assert(Pipe.hasRoomForWrite(target)) : "checked earlier should not fail";
							    			
							    			Pipe.addMsgIdx(target, NetPayloadSchema.MSG_PLAIN_210);
							    			Pipe.addLongValue(cc.getId(), target);         //connection
							    			Pipe.addLongValue(System.nanoTime(), target);
							    			Pipe.addLongValue(KNOWN_BLOCK_ENDING, target); //position
							    			
							    			int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(target);
							    			Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int)readCount, target);
				 				 
											
											if (showResponse) {
														    System.err.println("//////////////////////");
												   			Appendables.appendUTF8(System.err, target.blobRing, originalBlobPosition, readCount, target.blobMask);
												   			System.err.println("//////////////////////");
											}
		
							    			
							    			Pipe.confirmLowLevelWrite(target, SIZE_OF_PLAIN);
							    			Pipe.publishWrites(target);
					    													    			
							    		}						    		
							    		
							    	} else {
							    		//logger.info("zero read detected client side..");
							    		//nothing to send so let go of byte buffer.
							    		Pipe.unstoreBlobWorkingHeadPosition(target);
							    	}
						    	}
					    	} else {
					    		//not an error, just try again later.
					    		
								 if (--maxWarningCount>0) {//this should not be a common error but needs to be here to promote good configurations
					    				logger.warn("odd we should not be here for this test.");
					    			}
					    	}
					    } 
					}	

			} while(didWork);

				
//		boolean debug = false;
//		if (debug) {
//			if (lastTotalBytes!=totalBytes) {
//				System.err.println("Client reader total bytes :"+totalBytes);
//				lastTotalBytes =totalBytes;
//			}
//		}
	}



	long lastTotalBytes = 0;

	
   //must be called often to keep empty.
	private boolean consumeRelease() {
		
		boolean didWork = false;
		int i = releasePipes.length;
		while (--i>=0) {			
			Pipe<ReleaseSchema> ack = releasePipes[i];
			
			while (Pipe.hasContentToRead(ack)) {
				
				didWork = true;
				
				int id = Pipe.takeMsgIdx(ack);
				if (id == ReleaseSchema.MSG_RELEASE_100) {
					
					long fieldConnectionId = Pipe.takeLong(ack);
					long fieldPosition = Pipe.takeLong(ack);

					consumeRelease(fieldConnectionId, fieldPosition);
	    			
	    			Pipe.confirmLowLevelRead(ack, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASE_100));
				} else if (id == ReleaseSchema.MSG_RELEASEWITHSEQ_101) {
					
					long fieldConnectionID = Pipe.takeLong(ack);
					long fieldPosition = Pipe.takeLong(ack);
					int fieldSequenceNo = Pipe.takeInt(ack);
					
					consumeRelease(fieldConnectionID, fieldPosition);
					
					Pipe.confirmLowLevelRead(ack, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASEWITHSEQ_101));
				}else {
					assert(-1 == id) : "unexpected id of "+id;
					Pipe.confirmLowLevelRead(ack, Pipe.EOF_SIZE);
				}
				Pipe.releaseReadLock(ack);
			}
			
		}
		return didWork;
	}

	public void consumeRelease(long fieldConnectionId, long fieldPosition) {
		///////////////////////////////////////////////////
		//if sent tail matches the current head then this pipe has nothing in flight and can be re-assigned
		int pipeIdx = coordinator.checkForResponsePipeLineIdx(fieldConnectionId);
		if (pipeIdx>=0 && Pipe.workingHeadPosition(output[pipeIdx]) == fieldPosition) {
			assert(Pipe.contentRemaining(output[pipeIdx])==0) : "unexpected content on pipe detected";
			assert(!Pipe.isInBlobFieldWrite(output[pipeIdx])) : "unexpected open blob field write detected";
			
			coordinator.releaseResponsePipeLineIdx(fieldConnectionId);

		}
	}

}
