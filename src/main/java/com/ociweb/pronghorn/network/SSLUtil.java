package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class SSLUtil {

	final static ByteBuffer noData = ByteBuffer.allocate(0);
	final static ByteBuffer[] noDatas = new ByteBuffer[]{noData,noData};
	private static final Logger logger = LoggerFactory.getLogger(SSLUtil.class);
	
	public static boolean handShakeWrapIfNeeded(SSLConnection cc, Pipe<NetPayloadSchema> target, ByteBuffer buffer) {
		 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();
		 
		 
		 while (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus &&
				 HandshakeStatus.FINISHED != handshakeStatus	 ) {
			 			 
			 if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {
				 return true;//done by the other stage.
			 }
			 
			 if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
				 SSLUtil.handshakeWrapLogic(cc, target, buffer);
				 handshakeStatus = cc.getEngine().getHandshakeStatus();
				 return true;
			 }
			 
			 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
				 
	                Runnable task;
	                while ((task = cc.getEngine().getDelegatedTask()) != null) {
	                	task.run(); //NOTE: could be run in parallel but we only have 1 thread now
	                }
	                handshakeStatus = cc.getEngine().getHandshakeStatus();
	                
	                return (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus) && (HandshakeStatus.FINISHED != handshakeStatus);
			 }
		 }
		 return false;
	}
	
	public static void handshakeWrapLogic(SSLConnection cc, Pipe<NetPayloadSchema> target, ByteBuffer buffer) {
	    
		try {
			
			//TODO: must guarantee room to complete before we start.
			
			do {
				
				//TODO: very odd hack here.
				while (Pipe.contentRemaining(target)>0) {//  !PipeWriter.hasRoomForWrite(target)) {
					
				}
				
				final ByteBuffer[] targetBuffers = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
			
				SSLEngineResult result = cc.getEngine().wrap(noDatas, targetBuffers[0]);
				Status status = SSLUtil.wrapResultStatusState(target, buffer, cc, noDatas, targetBuffers, result);
				
				if (Status.OK == status) {
					
					PipeWriter.writeLong(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201, cc.getId());
					
					PipeWriter.publishWrites(target);
					HandshakeStatus handshakeStatus = result.getHandshakeStatus();
					
					//logger.info("wrapcalled  {} bytes sent {}",handshakeStatus, result.bytesProduced());
					
					
				} else {
				
					
					///TODO: release the open buffer?
					
					logger.info("unable to wrap {} {}",status, cc.getClass().getSimpleName(), new Exception());
							
					
				}
			
				//TODO: can the wraps be one message on outgoign pipe??? yes as long as we have wrap requesets in a row, close on first non wrap request. faster as well.
				
				
			} while(cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_WRAP); //what about TODO: outgoing pipe will fill up?
			
					
		} catch (SSLException e) {
			logger.error("unable to wrap ", e);
			PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);
		}	
	    
	}

	static void manageException(SSLException sslex, SSLConnection cc) {
		try {
			cc.close();
		} catch (Throwable t) {
			//ignore we are closing this connection
		}
		ClientConnection.log.error("Unable to encrypt closed conection",sslex);
	}

	public static Status wrapResultStatusState(Pipe<NetPayloadSchema> target, ByteBuffer buffer,
			final SSLConnection cc, ByteBuffer[] bbHolder, final ByteBuffer[] targetBuffers,
			SSLEngineResult result) {
		
		Status status = result.getStatus();
		if (status==Status.OK) {
			PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_ENCRYPTED_200);
			PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203, result.bytesProduced());
	
		} else if (status==Status.CLOSED){
							
			PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);							
	
			try {
				 cc.getEngine().closeOutbound();
				 
				 handShakeWrapIfNeeded(cc, target, buffer);
				 
				 cc.getSocketChannel().close();
			} catch (IOException e) {
				cc.close();
				ClientConnection.log.warn("Error closing connection ",e);
			}				
		} else if (status==Status.BUFFER_OVERFLOW) {
		
			
			///////////
			//This is only needed becauae engine.wrap does not take multiple target ByteBuffers as it should have.
			///////////
			try {
				buffer.clear();
				
				logger.info(" source buffer position:{} limit:{} ", bbHolder[0].position(), bbHolder[0].limit());
				logger.info(" source buffer position:{} limit:{} ", bbHolder[1].position(), bbHolder[1].limit());
				
				logger.info(" target buffer position:{} limit:{} ", buffer.position(), buffer.limit());
						
				
				SSLEngineResult result2 = cc.getEngine().wrap(bbHolder, buffer);
				
				status = result2.getStatus();
				if (status == Status.OK) {
					
					//write buffer to openA and openB
					buffer.flip();
					
					copyBufferIntoTwoBuffers(buffer, targetBuffers);
										
					PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_ENCRYPTED_200);
					PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203, result2.bytesProduced());
					
				} else {
					throw new UnsupportedOperationException("unexpected status of "+status);
					//ERROR?
					
				}
				
			} catch (SSLException sslex) {
				manageException(sslex, cc);			
				status = null;
			}
			
			
		} else {
			throw new RuntimeException("case should not happen, we have too little data to be wrapped and sent");
		}
		return status;
	}

	private static void copyBufferIntoTwoBuffers(ByteBuffer buffer, final ByteBuffer[] targetBuffers) {
		int finalLimit = buffer.limit();
		int room = targetBuffers[0].remaining();
		if (room<finalLimit) {
			buffer.limit(room);
		}										
		targetBuffers[0].put(buffer);
		buffer.limit(finalLimit);
		if (buffer.hasRemaining()) {
			targetBuffers[1].put(buffer);
		}
		assert(!buffer.hasRemaining());
	}

	private static SSLEngineResult gatherPipeDataForUnwrap(Pipe<NetPayloadSchema> source, ByteBuffer rolling,
			SSLConnection cc, final ByteBuffer[] targetBuffer) {
		SSLEngineResult result=null;
				
		ByteBuffer[] inputs = PipeReader.wrappedUnstructuredLayoutBuffer(source, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
		
		cc.localRunningBytesProduced = 0;
		if (inputs[1].remaining()==0) {
			//if we have some rolling data from previously
			if (rolling.position()==0) {
				try {					
					result = cc.getEngine().unwrap(inputs[0], targetBuffer);//common case where we can unwrap directly from the pipe.
					cc.localRunningBytesProduced += result.bytesProduced();
									
				} catch (SSLException sslex) {
					manageException(sslex, cc);	//the connection is closed
					
					rolling.clear();
					return null; //TODO: need to continue with new connections
					
				}				
				rolling.put(inputs[0]);//keep anything left for next time.
				assert(0==inputs[0].remaining());
				
			} else {
				//add this new content onto the end before use.
				rolling.put(inputs[0]);
				assert(0==inputs[0].remaining());
			}
		} else {
			assert(inputs[0].hasRemaining());
			assert(inputs[1].hasRemaining());
			
			rolling.put(inputs[0]); 
			rolling.put(inputs[1]);  
		}
		return result;
	}
	
//	SSLEngineUnWrapStage id:6] ERROR com.ociweb.pronghorn.network.ClientConnection - Unable to encrypt closed conection
//	javax.net.ssl.SSLException: Received fatal alert: unknown_ca
//		at sun.security.ssl.Alerts.getSSLException(Alerts.java:208)
//		at sun.security.ssl.SSLEngineImpl.fatal(SSLEngineImpl.java:1666)
//		at sun.security.ssl.SSLEngineImpl.fatal(SSLEngineImpl.java:1634)
//		at sun.security.ssl.SSLEngineImpl.recvAlert(SSLEngineImpl.java:1800)
//		at sun.security.ssl.SSLEngineImpl.readRecord(SSLEngineImpl.java:1083)
//		at sun.security.ssl.SSLEngineImpl.readNetRecord(SSLEngineImpl.java:907)
//		at sun.security.ssl.SSLEngineImpl.unwrap(SSLEngineImpl.java:781)
//		at javax.net.ssl.SSLEngine.unwrap(SSLEngine.java:664)
//		at com.ociweb.pronghorn.network.SSLUtil.gatherPipeDataForUnwrap(SSLUtil.java:214)
//		at com.ociweb.pronghorn.network.SSLUtil.handShakeUnWrapIfNeeded(SSLUtil.java:346)
//		at com.ociweb.pronghorn.network.SSLUtil.engineUnWrap(SSLUtil.java:470)
//		at com.ociweb.pronghorn.network.SSLEngineUnWrapStage.run(SSLEngineUnWrapStage.java:69)
//		at com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler.runPeriodicLoop(ThreadPerStageScheduler.java:455)
//		at com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler.access$6(ThreadPerStageScheduler.java:416)
//		at com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler$3.run(ThreadPerStageScheduler.java:334)
//		at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
//		at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
//		at java.lang.Thread.run(Thread.java:745)
//	[SSLEngineUnWrapStage id:6] INFO com.ociweb.pronghorn.network.SSLUtil - handshake NEED_UNWRAP 223289fd[SSLEngine[hostname=null port=-1] SSL_NULL_WITH_NULL_NULL]

	
			
	private static SSLEngineResult unwrapRollingHandshake(ByteBuffer rolling, SSLConnection cc, final ByteBuffer[] targetBuffer,	SSLEngineResult result) throws SSLException {
		while (cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_UNWRAP ||
			   cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_TASK) {				
															
			if (cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
				Runnable task;
				while ((task = cc.getEngine().getDelegatedTask()) != null) {
					assert(cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_TASK);
					task.run(); //NOTE: could be run in parallel but we only have 1 thread now
				}
			} else {
				assert(cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_UNWRAP);
				result = cc.getEngine().unwrap(rolling, targetBuffer);						
				
				int bytesProduced = result.bytesProduced();
				cc.localRunningBytesProduced += bytesProduced;

				if (result.getStatus() != Status.OK) {
					break;
				} 				
										
			}
		}
		//System.err.println("exit with "+cc.getEngine().getHandshakeStatus()); //TODO: THIS IS NOT HANDSHAKING YET WE ARE CHECKING FOR IT HERE!!!
		
		if (rolling.hasRemaining()) {
			rolling.compact(); //ready for append
			//logger.info("has data rolling buffer {} {} {}",rolling.position(),rolling.limit(),rolling.capacity()); 

		} else {
			//logger.info("CLEAR");
			rolling.clear();
		}
		
		return result;
	}
	
	private static SSLEngineResult unwrapRollingNominal(ByteBuffer rolling, SSLConnection cc, final ByteBuffer[] targetBuffer,	SSLEngineResult result) throws SSLException {
		while (rolling.hasRemaining()) {				
				result = cc.getEngine().unwrap(rolling, targetBuffer);						
				
				int bytesProduced = result.bytesProduced();
				cc.localRunningBytesProduced += bytesProduced;

				if (result.getStatus() != Status.OK) {
					if (result.getStatus() != Status.BUFFER_UNDERFLOW) {
						logger.error("bad result of {}",result.getStatus());
					}
					break;
				}
		}
		//System.err.println("exit with "+cc.getEngine().getHandshakeStatus()); //TODO: THIS IS NOT HANDSHAKING YET WE ARE CHECKING FOR IT HERE!!!
		
		if (rolling.hasRemaining()) {
			rolling.compact();
		} else {
			rolling.clear();
		}
		
		return result;
	}
	

	public static boolean handShakeUnWrapIfNeeded(SSLConnection cc, final Pipe<NetPayloadSchema> source, ByteBuffer rolling, ByteBuffer[] workspace, 
			                                      Pipe<NetPayloadSchema> handshakePipe, ByteBuffer secureBuffer) {
		
		assert(source!=null);  
		 boolean didShake = false;
		 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();		 
		 while (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus && HandshakeStatus.FINISHED != handshakeStatus) {
			 didShake = true;
			 
			 logger.info("handshake {} {}",handshakeStatus,cc.getEngine());
		
			 if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
				 if (null == handshakePipe) {
					 //Client Mode
					 break;
				 } else {
					 //Server Mode
					 //do not return instead go around loop to ensure task or other work is not needed
					 handshakeWrapLogic(cc, handshakePipe, secureBuffer);
					 handshakeStatus = cc.getEngine().getHandshakeStatus();	
				 }
			 } else
			 
			 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
	                Runnable task;
	                while ((task = cc.getEngine().getDelegatedTask()) != null) {
	                	task.run(); //NOTE: could be run in parallel but we only have 1 thread now
	                }
	                handshakeStatus = cc.getEngine().getHandshakeStatus();
			 } else
			 
			 if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {
				 	if (!PipeReader.tryReadFragment(source)) {
				 		break;
				 	}				 	
				 	
				 	int msgId = PipeReader.getMsgIdx(source);
				 					 	
				    //if closed closd
	                if (msgId<0 || !cc.isValid) {	                	
	                	PipeReader.releaseReadLock(source);
	                    if (cc.getEngine().isInboundDone() && cc.getEngine().isOutboundDone()) {	                    	
	                        break;
	                    }	                    
	                    handshakeStatus = cc.closeInboundCloseOutbound(cc.getEngine());	                   
	                    break; //TODO: not sure this is right.
	                }
					final ByteBuffer[] targetBuffer = workspace;
					SSLEngineResult result1 = gatherPipeDataForUnwrap(source, rolling, cc, targetBuffer);
								
					rolling.flip();	
					
					try {
						result1 = unwrapRollingHandshake(rolling, cc, targetBuffer, result1);
					} catch (SSLException sslex) {
						logger.warn("needed handshake status of {}", cc.getEngine().getHandshakeStatus());						
						manageException(sslex, cc);
						logger.warn("error on {} side",  (null==handshakePipe ? "client" : "server"));
						return true;//connection dead but do continue
					}
					
					PipeReader.releaseReadLock(source);
	    			
	    			SSLEngineResult result = result1;
	    	
	    			handshakeStatus = result.getHandshakeStatus();
					Status status = null!=result?result.getStatus():null;
	
					if (Status.CLOSED == status) {
						 if (cc.getEngine().isOutboundDone()) {
							 	cc.close();
		                    	break;
		                    } else {
		                        cc.getEngine().closeOutbound();
		                        handshakeStatus = cc.getEngine().getHandshakeStatus();
		                    }
					}					
			 }
		 }
		 return didShake;
	}

	/**
	 * Encrypt as much as possible based on the data available from the two pipes
	 * @param source
	 * @param target
	 * @param buffer 
	 * @param groupId TODO
	 */
	public static void engineWrap(SSLConnectionHolder ccm, Pipe<NetPayloadSchema> source, Pipe<NetPayloadSchema> target, ByteBuffer buffer, boolean isServer, int groupId) {
		
		while (PipeWriter.hasRoomForWrite(target) && PipeReader.peekMsg(source, NetPayloadSchema.MSG_PLAIN_210) ) {
			
			final SSLConnection cc = ccm.get(PipeReader.peekLong(source, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201), groupId);
						
			if (null==cc || !cc.isValid) {
				//do not process this message because the connection has dropped
				PipeReader.tryReadFragment(source);
				PipeReader.releaseReadLock(source);
				continue;
			}
			
			//logger.info("wrap if needed");
			
			if ((!isServer) && handShakeWrapIfNeeded(cc, target, buffer)) {
				return;
			}
			
			PipeReader.tryReadFragment(source);
			
			ByteBuffer[] bbHolder = PipeReader.wrappedUnstructuredLayoutBuffer(source, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);										
	
			try {
				final ByteBuffer[] targetBuffers = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
				
				//TODO: wrap may not be given any room URGENT!!
				SSLEngineResult result = cc.getEngine().wrap(bbHolder, targetBuffers[0]);			
				Status status = wrapResultStatusState(target, buffer, cc, bbHolder, targetBuffers, result);
											
				if (status == Status.OK) {
					
					PipeReader.copyLong(source, target, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, 
							                            NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201);
					
					
					if (bbHolder[0].remaining()>0) {
						throw new RuntimeException("did not consume all the input data "+bbHolder[0].remaining()+" "+bbHolder[1].remaining());
					}
					
					if (bbHolder[1].remaining()>0) {
						throw new RuntimeException("did not consume all the input data "+bbHolder[1].remaining());						
					}
					
					
					PipeWriter.publishWrites(target);
					PipeReader.releaseReadLock(source);	//TODO: can we release this not sure??
					
				} else if (status == Status.CLOSED) {
					
					PipeReader.releaseReadLock(source);		
					
				}
			
			} catch (SSLException sslex) {
				manageException(sslex, cc);			
				
			}
			
			
		}
	
	}

	public static void engineUnWrap(SSLConnectionHolder ccm, Pipe<NetPayloadSchema> source, Pipe<NetPayloadSchema> target, 
			                        ByteBuffer rolling, ByteBuffer[] workspace, Pipe<NetPayloadSchema> handshakePipe, ByteBuffer secureBuffer, int groupId) {
		
		
		while (PipeReader.hasContentToRead(source) ) {
			
			//TODO: check the handshake output for backed up data.
			if (!PipeWriter.hasRoomForWrite(target)) {								
				return;//try again later when there is room in the output
			}			
				
			
			SSLConnection cc = ccm.get(PipeReader.peekLong(source, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201), groupId);
			
			if (null==cc || !cc.isValid) {
				//do not process this message because the connection has dropped
				PipeReader.tryReadFragment(source);
				PipeReader.releaseReadLock(source);
				continue;
			}

			//need to come back in for needed wrap even without content to read but... we need the content to give use the CC !!!
			if (handShakeUnWrapIfNeeded(cc, source, rolling, workspace, handshakePipe, secureBuffer)) {				
				return;
			}
			
			if (!PipeReader.tryReadFragment(source)) {
				return;//not an error because the only fragment may have been consumed by the handshake
			}
			
			if (PipeReader.getMsgIdx(source)<0) {
				PipeWriter.publishEOF(target);
				PipeReader.releaseReadLock(source);
				//requestShutdown(); //TODO: shutdown is not propigated due to not having stage here.
				return;
			}
			
			if (PipeReader.getMsgIdx(source) == NetPayloadSchema.MSG_DISCONNECT_203) {
				PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_DISCONNECT_203);
				PipeWriter.writeLong(target, NetPayloadSchema.MSG_DISCONNECT_203_FIELD_CONNECTIONID_201, PipeReader.readLong(source, NetPayloadSchema.MSG_DISCONNECT_203_FIELD_CONNECTIONID_201));				
				PipeWriter.publishWrites(target);
				PipeReader.releaseReadLock(source);
				return;
			}
			assert(PipeReader.getMsgIdx(source) == NetPayloadSchema.MSG_ENCRYPTED_200);
			
			final ByteBuffer[] writeHolderUnWrap = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
			SSLEngineResult result1 = gatherPipeDataForUnwrap(source, rolling, cc, writeHolderUnWrap);
			
			if (rolling.position()!=0) {		
				rolling.flip();	
				
				try {
					result1 = unwrapRollingNominal(rolling, cc, writeHolderUnWrap, result1);
				} catch (SSLException sslex) {
					manageException(sslex, cc);
					result1 = null;
					
				}
			}
			
			PipeReader.releaseReadLock(source);
			
			
			SSLEngineResult result = result1;
			Status status = null==result?null:result.getStatus();			
			
			if(cc.localRunningBytesProduced>0) {
				if (!PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_PLAIN_210)) {
					throw new RuntimeException("already checked for space should not happen.");
				}
				PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204, cc.localRunningBytesProduced);
				cc.localRunningBytesProduced = -1;
				
				//assert(longs match the one for cc)
				PipeWriter.writeLong(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, cc.getId());
				//    PipeReader.copyLong(source, target, ClientNetResponseSchema.MSG_RESPONSE_200_FIELD_CONNECTIONID_201, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210_FIELD_CONNECTIONID_201);
				
				PipeWriter.publishWrites(target);
				
			}			
			
			if (status==Status.OK || status==null) {						
	
			} else if (status==Status.CLOSED){
				if (cc.localRunningBytesProduced==0) {
					PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);							
				} else {
					System.err.println("AAA Xxxxxxxxxxxxxxxxxx found some data to send ERROR: must publish this");					
					
				}
				
				try {
					 cc.getEngine().closeOutbound();
					 handShakeUnWrapIfNeeded(cc, source, rolling, workspace, handshakePipe, secureBuffer);
				     cc.getSocketChannel().close();
				} catch (IOException e) {
					cc.isValid = false;
					ClientConnection.log.warn("Error closing connection ",e);
				}				
			} else if (status==Status.BUFFER_UNDERFLOW) {
				
				//roller already contains previous so no work but to cancel the outgoing write
				if (cc.localRunningBytesProduced==0) {
					PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);							
				} else {
					if (cc.localRunningBytesProduced>0) {
						if (!PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_PLAIN_210)) {
							throw new RuntimeException("already checked for space should not happen.");
						}
						PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204, cc.localRunningBytesProduced);
						cc.localRunningBytesProduced = -1;
						
						//assert(longs match the one for cc)
						PipeWriter.writeLong(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, cc.getId());
						//    PipeReader.copyLong(source, target, ClientNetResponseSchema.MSG_RESPONSE_200_FIELD_CONNECTIONID_201, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210_FIELD_CONNECTIONID_201);
						
						PipeWriter.publishWrites(target);
					}
				}
			} else {				
				assert(status == Status.BUFFER_OVERFLOW);
				
				throw new RuntimeException("server is untrustworthy? Output pipe is too small for the content to be written inside "+
			                               target.maxAvgVarLen+" reading from "+rolling.position());
				
	
			}	
		}
		
		
		
	}

}
