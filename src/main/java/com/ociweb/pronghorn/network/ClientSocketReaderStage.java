package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

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

	private long start;
	private long totalBytes=0;

	private final static int KNOWN_BLOCK_ENDING = -1;

	private final int maxClients;
	

	
	private StringBuilder[] accumulators; //for testing only
	
	
	
	public ClientSocketReaderStage(GraphManager graphManager, ClientCoordinator coordinator, Pipe<ReleaseSchema>[] parseAck, Pipe<NetPayloadSchema>[] output) {
		super(graphManager, parseAck, output);
		this.coordinator = coordinator;
		this.output = output;
		this.releasePipes = parseAck;

		this.maxClients = coordinator.maxClientConnections();		
		
		coordinator.setStart(this);
		
	}
	
	@Override
	public void startup() {
		start = System.currentTimeMillis();
		
		if (ClientCoordinator.TEST_RECORDS) {
			int i = output.length;
			accumulators = new StringBuilder[i];
			while (--i >= 0) {			
				accumulators[i]=new StringBuilder();					
			}
		}
		
	}
	
	@Override
	public void shutdown() {
		long duration = System.currentTimeMillis()-start;
		
		logger.trace("Client Bytes Read: {} kb/sec {} ",totalBytes, (8*totalBytes)/duration);
		
	}

	int maxWarningCount = 10;
	
	@Override
	public void run() {

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
					    	int pipeIdx = coordinator.responsePipeLineIdx(cc.getId());//picks any open pipe to keep the system busy
					    	if (pipeIdx>=0) {					    		
					    	} else {				    	
					    		consumeRelease();
					    		pipeIdx = coordinator.responsePipeLineIdx(cc.getId()); //try again.
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
						        		
						    								    		
						    		//these buffers are only big enought to accept 1 target.maxAvgVarLen
						    		ByteBuffer[] wrappedUnstructuredLayoutBufferOpen = Pipe.wrappedWritingBuffers(target);
						    
//						    		int r1 = wrappedUnstructuredLayoutBufferOpen[0].remaining();
//						    		int r2 = wrappedUnstructuredLayoutBufferOpen[1].remaining();
						    		
						    		
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
				 				 
									//		boolean showResponse = true;
									//		if (showResponse) {
									//			   			Appendables.appendUTF8(System.err, target.blobRing, originalBlobPosition, readCount, target.blobMask);
									//		}
		
							    			if (ClientCoordinator.TEST_RECORDS) {
							    				validateContent(pipeIdx, target, readCount, originalBlobPosition);
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

	private void validateContent(int pipeIdx, Pipe<NetPayloadSchema> target, int readCount, int originalBlobPosition) {
		
		if (ClientCoordinator.TEST_RECORDS) {
			
			//write pipeIdx identifier.
			//Appendables.appendUTF8(System.out, target.blobRing, originalBlobPosition, readCount, target.blobMask);
			
			
			boolean confirmExpectedRequests = true;
			if (confirmExpectedRequests) {
				Appendables.appendUTF8(accumulators[pipeIdx], target.blobRing, originalBlobPosition, readCount, target.blobMask);						    				
				
				while (accumulators[pipeIdx].length() >= ClientCoordinator.expectedOK.length()) {
					
				   int c = startsWith(accumulators[pipeIdx],ClientCoordinator.expectedOK); 
				   if (c>0) {
					   
					   String remaining = accumulators[pipeIdx].substring(c*ClientCoordinator.expectedOK.length());
					   accumulators[pipeIdx].setLength(0);
					   accumulators[pipeIdx].append(remaining);							    					   
					   
					   
				   } else {
					   logger.info("A"+Arrays.toString(ClientCoordinator.expectedOK.getBytes()));
					   logger.info("B"+Arrays.toString(accumulators[pipeIdx].subSequence(0, ClientCoordinator.expectedOK.length()).toString().getBytes()   ));
					   
					   logger.info("FORCE EXIT ERROR at {} exlen {}",originalBlobPosition,ClientCoordinator.expectedOK.length());
					   System.out.println(accumulators[pipeIdx].subSequence(0, ClientCoordinator.expectedOK.length()).toString());
					   System.exit(-1);
					   	
					   
					   
				   }
				
					
				}
			}
			
			
		}
	}
	

	
	
	private int startsWith(StringBuilder stringBuilder, String expected2) {
		
		int count = 0;
		int rem = stringBuilder.length();
		int base = 0;
		while(rem>=expected2.length()) {
			int i = expected2.length();
			while (--i>=0) {
				if (stringBuilder.charAt(base+i)!=expected2.charAt(i)) {
					return count;
				}
			}
			base+=expected2.length();
			rem-=expected2.length();
			count++;
		}
		return count;
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
