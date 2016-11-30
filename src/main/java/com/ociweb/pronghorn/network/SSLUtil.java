package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

import com.ociweb.pronghorn.network.schema.NetParseAckSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class SSLUtil {

	final static ByteBuffer noData = ByteBuffer.allocate(0);
	final static ByteBuffer[] noDatas = new ByteBuffer[]{noData,noData};
	private static final Logger logger = LoggerFactory.getLogger(SSLUtil.class);
	

    public static final long HANDSHAKE_TIMEOUT = 20_000_000_000L; // 20 sec, this is a very large timeout for handshake to complete.
    public static final long HANDSHAKE_POS = -123;
    
	
	public static boolean handShakeWrapIfNeeded(SSLConnection cc, Pipe<NetPayloadSchema> target, ByteBuffer buffer, boolean isServer) {
		//synchronized (cc.engine) {
				
		 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();
		 
		 boolean didShake = false;
		 while (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus &&
				 HandshakeStatus.FINISHED != handshakeStatus	 ) {
			 			 
			 didShake = true;
			 if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {
				 
				 long nsDuration = cc.durationWaitingForNetwork();
				 if (nsDuration > HANDSHAKE_TIMEOUT) {
					 logger.warn("Handshake wrap abanonded for {} due to timeout of {} ns waiting for unwrap done by other stage.",cc,HANDSHAKE_TIMEOUT);
					 cc.close();					 
					 System.exit(-1);
				 }
				 return true;//done by the other stage.
			 }
			 
			 if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
				 SSLUtil.handshakeWrapLogic(cc, target, buffer, isServer);
				 handshakeStatus = cc.getEngine().getHandshakeStatus();
				 //return true;
			 }
			 
			 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
	             Runnable task;
	             while ((task = cc.getEngine().getDelegatedTask()) != null) {
	                	task.run(); //NOTE: could be run in parallel but we only have 1 thread now
	             }
	             handshakeStatus = cc.getEngine().getHandshakeStatus();
	                
	               // return (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus) && (HandshakeStatus.FINISHED != handshakeStatus);
			 }
		 }
		 cc.clearWaitingForNetwork();
		// logger.info("status is now "+handshakeStatus);
		 return didShake;//false
	  // }
	}
	
	public static void handshakeWrapLogic(SSLConnection cc, Pipe<NetPayloadSchema> target, ByteBuffer buffer, boolean isServer) {
	    
		try {
			
			//TODO: must guarantee room to complete before we start.
			
			do {
				
				//TODO: very odd hack here. for handshake, needs to be redesigned to drop this loop.
				while (!PipeWriter.hasRoomForWrite(target)) {
					Thread.yield();
				}
				
				final ByteBuffer[] targetBuffers = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
			

				Status status = SSLUtil.wrapResultStatusState(target, buffer, cc, noDatas, targetBuffers, isServer);
				
				if (Status.OK == status) {
					
					PipeWriter.writeLong(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201, cc.getId());
					
					PipeWriter.publishWrites(target);
				} else {
					//connection was closed before handshake completed 
				    if (Status.CLOSED == status) {
				    	//no need to cancel wrapped buffer it was already done by wrapResultStatusState
				    	return;
				    }
					logger.info("HANDSHAKE unable to wrap {} {} {} ",status, cc.getClass().getSimpleName(), cc.engine, new Exception());
					throw new RuntimeException();		
					
				}
			
				//TODO: can the wraps be one message on outgoign pipe??? yes as long as we have wrap requesets in a row, close on first non wrap request. faster as well.
				
				
			} while(cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_WRAP); //what about TODO: outgoing pipe will fill up?
			
					
		} catch (SSLException e) {
			logger.error("unable to wrap ", e);
			PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);
		}	
	    
	}

	static void manageException(SSLException sslex, SSLConnection cc, boolean isServer) {
		try {
			cc.close();
		} catch (Throwable t) {
			//ignore we are closing this connection
		}
		ClientConnection.log.error("Unable to encrypt closed conection, in server:{}",sslex,isServer);
	}

	public static Status wrapResultStatusState(Pipe<NetPayloadSchema> target, ByteBuffer buffer,
			final SSLConnection cc, ByteBuffer[] bbHolder, final ByteBuffer[] targetBuffers, boolean isServer) throws SSLException {
	
		if (cc.getEngine().isOutboundDone()) {
			PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);	
			return Status.CLOSED; 
		}
		
		SSLEngineResult result = cc.getEngine().wrap(bbHolder, targetBuffers[0]);
		
		Status status = result.getStatus();
		
		//logger.info("status state {}",status);
		if (status==Status.OK) {
			PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_ENCRYPTED_200);
			PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203, result.bytesProduced());
	
		} else if (status==Status.CLOSED){
							
			PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);							
	
			try {
				 cc.getEngine().closeOutbound();				 
				 handShakeWrapIfNeeded(cc, target, buffer, isServer);				 
				 cc.getSocketChannel().close();
				 cc.close();
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
				SSLEngineResult result2 = cc.getEngine().wrap(bbHolder, buffer);
				
				status = result2.getStatus();
				if (status == Status.OK) {
					
					//write buffer to openA and openB
					buffer.flip();
					
					copyBufferIntoTwoBuffers(buffer, targetBuffers);
										
					PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_ENCRYPTED_200);
					PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203, result2.bytesProduced());
					
				} else {
					PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);	
					if (status == Status.CLOSED) {
						return status;
					} else {
						throw new UnsupportedOperationException("unexpected status of "+status);
						//ERROR?
					}
				}
				
			} catch (SSLException sslex) {
				manageException(sslex, cc, isServer);			
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
															SSLConnection cc, final ByteBuffer[] targetBuffer, boolean isServer) {
		SSLEngineResult result=null;
				
		//TODO: upon exception this is not getting closed !!
		ByteBuffer[] inputs = PipeReader.wrappedUnstructuredLayoutBuffer(source, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
		
		cc.localRunningBytesProduced = 0;
		if (inputs[1].remaining()==0) {
			//if we have some rolling data from previously
			ByteBuffer inZero = inputs[0];
			if (rolling.position()==0) {
				try {					
				//
					result = unwrap(cc, targetBuffer, inZero);
					
				} catch (SSLException sslex) {
					manageException(sslex, cc, isServer);	//the connection is closed
					
					rolling.clear();
					return null; //TODO: need to continue with new connections
					
				}				
				rolling.put(inZero);//keep anything left for next time.
				assert(0==inZero.remaining());
				
				
			} else {
				
				//add this new content onto the end before use.				
				rolling.put(inZero);
				assert(0==inZero.remaining());
			}
		} else {
			assert(inputs[0].hasRemaining());
			assert(inputs[1].hasRemaining());
			
			rolling.put(inputs[0]); 
			rolling.put(inputs[1]);  
	
		}
		return result;
	}

	private static SSLEngineResult unwrap(SSLConnection cc, final ByteBuffer[] targetBuffer, ByteBuffer sourceBuffer)
			throws SSLException {
		SSLEngineResult result;
		int origLimit;
		do {
			///////////////
			//Block needed for limitations of OpenSSL, can only  support small blocks to be decryptded at a time
			///////////////
			origLimit = sourceBuffer.limit();
			int pos = sourceBuffer.position();
			if (origLimit-pos>SSLEngineFactory.maxEncryptedContentLength) {
				sourceBuffer.limit(pos+SSLEngineFactory.maxEncryptedContentLength);
			}
			/////////////
			
			assert(sourceBuffer.remaining()<=SSLEngineFactory.maxEncryptedContentLength);
			
			result = cc.getEngine().unwrap(sourceBuffer, targetBuffer);//common case where we can unwrap directly from the pipe.
			//System.err.println("B unwrapped bytes produced "+result.bytesProduced()+"  "+result.getStatus());
			
			sourceBuffer.limit(origLimit);//restore the limit so we can keep the remaining data (only critical for openSSL compatibility, see above)
			assert(cc.localRunningBytesProduced>=0);
			cc.localRunningBytesProduced += result.bytesProduced();
			
		
		} while(result.getStatus() == Status.OK && sourceBuffer.hasRemaining() && cc.getEngine().getHandshakeStatus()==HandshakeStatus.NOT_HANDSHAKING);
		return result;
	}

			
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
				
					do {
				//	    ////////////////////////
					    ///Block needed for openSSL limitation
					    ///////////////////////
					    int origLimit = rolling.limit();
					    int pos = rolling.position();
					    if (origLimit-pos>SSLEngineFactory.maxEncryptedContentLength) {
					    	rolling.limit(pos+SSLEngineFactory.maxEncryptedContentLength);
					    }
					    /////////////////////////
				
					 
						result = cc.getEngine().unwrap(rolling, targetBuffer);			
		
						//	System.err.println("C unwrapped bytes produced "+result.bytesProduced()+"  "+result.getStatus()+"  "+"  "+rolling.hasRemaining()+" "+cc.getEngine().getHandshakeStatus() );
						
						rolling.limit(origLimit); //return origLimit, see above openSSL issue.
						
						int bytesProduced = result.bytesProduced();
						assert(cc.localRunningBytesProduced>=0);
						cc.localRunningBytesProduced += bytesProduced;
				    } while (cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_UNWRAP && result.getStatus() == Status.OK && rolling.hasRemaining()); //may need data
	
					if (result.getStatus() != Status.OK || cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_UNWRAP) {
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
			
			
			    ////////////////////////
			    ///Block needed for openSSL limitation
			    ///////////////////////
			    int origLimit = rolling.limit();
			    int pos = rolling.position();
			    if (origLimit-pos>SSLEngineFactory.maxEncryptedContentLength) {
			    	rolling.limit(pos+SSLEngineFactory.maxEncryptedContentLength);
			    }
			    /////////////////////////
  
				result = cc.getEngine().unwrap(rolling, targetBuffer);		
				//System.err.println("A unwrapped bytes produced "+result.bytesProduced()+"  "+result.getStatus());
				
				rolling.limit(origLimit); //return origLimit, see above openSSL issue.
				
				int bytesProduced = result.bytesProduced();
				assert(cc.localRunningBytesProduced>=0);
				cc.localRunningBytesProduced += bytesProduced;
				
				if (result.getStatus() != Status.OK || cc.getEngine().getHandshakeStatus()!=HandshakeStatus.NOT_HANDSHAKING) {
					break;
				}
		}

		if (rolling.hasRemaining()) {
			if (rolling.position()!=0) {
				rolling.compact();
			}
		} else {
			rolling.clear();
		}
		
		return result;
	}
	

	public static boolean handShakeUnWrapIfNeeded(SSLConnection cc, final Pipe<NetPayloadSchema> source, ByteBuffer rolling, ByteBuffer[] workspace, 
			                                      Pipe<NetPayloadSchema> handshakePipe, ByteBuffer secureBuffer) {
		
		//synchronized (cc.engine) {
		 boolean isServer = null!=handshakePipe;	
		 assert(source!=null);  
		 boolean didShake = false;
		 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();		 
		 while (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus && HandshakeStatus.FINISHED != handshakeStatus) {
			 didShake = true;
			 
			 //logger.info("handshake {} {}",handshakeStatus,cc);
		
			 if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
				 if (null == handshakePipe) {
					 //Client Mode
					 break;
				 } else {
					 //Server Mode
					 //do not return instead go around loop to ensure task or other work is not needed
					 handshakeWrapLogic(cc, handshakePipe, secureBuffer, isServer);
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
					
				 
				 //TODO: unwrap the rolling value before reading next source.
				 //      but socket reader needs to know that handshake is not done so not to copy data... can only be fixed at socket reader?
				 
				 
				 //logger.info("server {} doing the unwrap now",isServer);
				 
				 	if (!PipeReader.tryReadFragment(source)) {
				 		if (cc.durationWaitingForNetwork()>HANDSHAKE_TIMEOUT) {
				 			logger.info("No data provided for unwrap. timed out after {} ",HANDSHAKE_TIMEOUT);
				 			cc.close();
				 		}
				 		break;
				 	}	
				 	cc.clearWaitingForNetwork();
				 	
				 	int msgId = PipeReader.getMsgIdx(source);
				 					 	
				    //if closed closd
	                if (msgId<0 || !cc.isValid) {	                	
	                	PipeReader.releaseReadLock(source);
	                    if (cc.getEngine().isInboundDone() && cc.getEngine().isOutboundDone()) {	
	                    	logger.info("close it was closed");
	                        break;
	                    }	                    
	                    handshakeStatus = cc.closeInboundCloseOutbound(cc.getEngine());	  
	                    logger.info("check this case??");
	                    break; //TODO: not sure this is right.
	                }
					final ByteBuffer[] targetBuffer = workspace;
					SSLEngineResult result1 = gatherPipeDataForUnwrap(source, rolling, cc, targetBuffer, isServer);
								
					rolling.flip();	
					//logger.info("server {} unwrap rolling data {} for {} ", isServer, rolling, cc);
					
					try {
						result1 = unwrapRollingHandshake(rolling, cc, targetBuffer, result1);
						//logger.info("server {} status is now {}  for {} ",isServer, cc.getEngine().getHandshakeStatus(),cc);
					} catch (SSLException sslex) {
						logger.warn("ERROR: needed handshake status of {} for server {}", cc.getEngine().getHandshakeStatus(), sslex, isServer);						
						manageException(sslex, cc, isServer);
						logger.warn("error on {} side",  (null==handshakePipe ? "client" : "server"));
						return true;//connection dead but do continue
					}
					
					//logger.info("server {} releaseing read lock for {} ",isServer,cc);
					PipeReader.releaseReadLock(source);
	    			
	    			SSLEngineResult result = result1;
	
					if (null==result || Status.CLOSED == result.getStatus()) {
						 if (cc.getEngine().isOutboundDone()) {
							 	cc.close();
							 	logger.info("yyyyyyyyyyyyyyyyyyy closed");
		                    	break;
		                    } else {
		                        cc.getEngine().closeOutbound();
		                        handshakeStatus = cc.getEngine().getHandshakeStatus();
		                    }
					} else {					
						handshakeStatus = cc.getEngine().getHandshakeStatus();
					}
			 }
		 }
		 return didShake;
		//}
	}

	/**
	 * Encrypt as much as possible based on the data available from the two pipes
	 * @param source
	 * @param target
	 * @param buffer 
	 * @param groupId TODO
	 */
	public static boolean engineWrap(SSLConnectionHolder ccm, Pipe<NetPayloadSchema> source, Pipe<NetPayloadSchema> target, ByteBuffer buffer, boolean isServer, int groupId) {
		
		boolean didWork = false;
				
		while (PipeWriter.hasRoomForWrite(target) && PipeReader.peekMsg(source, NetPayloadSchema.MSG_PLAIN_210) ) {
			
			final SSLConnection cc = ccm.get(PipeReader.peekLong(source, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201), groupId);
						
			if (null==cc || !cc.isValid) {
				logger.info("connection has dropped so no wrapping can be done");
				//do not process this message because the connection has dropped
				boolean ok = PipeReader.tryReadFragment(source);
				assert(ok);
				PipeReader.releaseReadLock(source);
				continue;
			}
			
			buffer.clear(); //buffer will not bring in ANY data.

			
			if ((!isServer) && handShakeWrapIfNeeded(cc, target, buffer, isServer)) {
				
				//we know the message is plain but what was the position? if this is an empty message just for handshake then clear it
				long pos = PipeReader.peekLong(source, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206);
				if (pos == HANDSHAKE_POS) {
					PipeReader.tryReadFragment(source);
					PipeReader.releaseReadLock(source);
					//todo assert 0 length
				}				
				
				return didWork;
			}
			
			boolean isRead= PipeReader.tryReadFragment(source);
			assert(isRead);
			
			ByteBuffer[] soruceBuffers = PipeReader.wrappedUnstructuredLayoutBuffer(source, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);										
	
			try {
				final ByteBuffer[] targetBuffers = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
				
				Status status = wrapResultStatusState(target, buffer, cc, soruceBuffers, targetBuffers, isServer);
											
				if (status == Status.OK) {
					didWork = true;
					
					PipeReader.copyLong(source, target, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, 
							                            NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201);
					
					
					if (soruceBuffers[0].remaining()>0) {
						throw new RuntimeException("did not consume all the input data "+soruceBuffers[0].remaining()+" "+soruceBuffers[1].remaining());
					}
					
					if (soruceBuffers[1].remaining()>0) {
						throw new RuntimeException("did not consume all the input data "+soruceBuffers[1].remaining());						
					}
					
					
					PipeWriter.publishWrites(target);
					PipeReader.releaseReadLock(source);	//can release lock since sourceBuffers are both empty
					
				} else if (status == Status.CLOSED) {
					
					PipeReader.releaseReadLock(source);		
				} else {
					
					PipeReader.releaseReadLock(source);		
					throw new RuntimeException("no release, unexpected status "+status);
				}
			
			} catch (SSLException sslex) {
				manageException(sslex, cc, isServer);			
				
			}
			
			
		}
		buffer.clear();
		
		return didWork;
	}

	public static int engineUnWrap(SSLConnectionHolder ccm, Pipe<NetPayloadSchema> source, Pipe<NetPayloadSchema> target, 
			                        ByteBuffer rolling, ByteBuffer[] workspace, Pipe<NetPayloadSchema> handshakePipe, Pipe<NetParseAckSchema> ack, 
			                        ByteBuffer secureBuffer, int groupId) {
		boolean isServer = null!=handshakePipe;
		int didWork = 0;
		boolean cameFromHandshake = false;
		while (PipeReader.hasContentToRead(source) ) {
			
			//TODO: check the handshake output for backed up data.
			if (!PipeWriter.hasRoomForWrite(target)) {								
				return didWork;//try again later when there is room in the output
			}			
				
			SSLConnection cc = null;
			if (!PipeReader.peekMsg(source, -1)) {
				cc = ccm.get(PipeReader.peekLong(source, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201), groupId);
				
				if (null==cc || !cc.isValid) {
					logger.info("sever {} ignored closed connection {}",isServer,cc);
					//do not process this message because the connection has dropped
					PipeReader.tryReadFragment(source);
					PipeReader.releaseReadLock(source);
					continue;
				}
	
//				if (cc.getId()<1 || cc.getId()>3) {
//				
//					System.out.println(cc.getId()+" "+rolling);
//				}
//				boolean isClient = null==handshakePipe;
//				System.err.println(cc.getId()+" "+System.identityHashCode(rolling)+"  "+source.id+" "+target.id+" is client "+isClient); //TODO: warning rolling is getting mixed!!
//				
				
				didWork = 1;
				
				//need to come back in for needed wrap even without content to read but... we need the content to give use the CC !!!
				if (handShakeUnWrapIfNeeded(cc, source, rolling, workspace, handshakePipe, secureBuffer)) {		
					//boring appears to be no faster and drops data,
					//logger.info("handshake data :"+cc.engine.getSession().getCipherSuite());//boring TLS_ECDHE_RSA_WITH_RC4_128_SHA    or   TLS_DHE_RSA_WITH_AES_256_CBC_SHA256 or TLS_DHE_RSA_WITH_AES_256_GCM_SHA384
					                                                                        //java   TLS_DHE_RSA_WITH_AES_128_GCM_SHA256
					
					//send ack back here OR does the sender know??
					if (null!=ack && rolling.position()==0 && Pipe.contentRemaining(source)==0) {														
						
			        	sendAck(source, ack, cc);			        	
						
					} else {
						cameFromHandshake = true;
						if (HandshakeStatus.NOT_HANDSHAKING != cc.getEngine().getHandshakeStatus()) {
							//TODO: need special ack for handshake that (similar to the other implmenation) is an ack only and does not contain any content.			
							logger.info("{} server {} will not release {} rolling is {} and pipe is {} ",cc.getEngine().getHandshakeStatus(), isServer, cc.id, rolling, source);
						}
						
						//TODO: else , why do we have extra data here???  127 bytes in roller or in source...
						//logger.info("{} server {} will not release {} rolling is {} and pipe is {} ",cc.getEngine().getHandshakeStatus(), isServer, cc.id, rolling, source);
						
					}
					
					
					if (HandshakeStatus.NOT_HANDSHAKING != cc.getEngine().getHandshakeStatus()) {
						return didWork;
					}
					//else stay since we have some data
					
				}	
			}

			if (!PipeReader.tryReadFragment(source)) {
				return 0;//not an error because the only fragment may have been consumed by the handshake
			}
			
			if (PipeReader.getMsgIdx(source)<0) {
				
				if (rolling.position()>0) {
					logger.info("shutdown of unwrap detected but we must procesing rolling data first {} isServer:{}",rolling,isServer);

					//Must send disconnect first to empty the data when we know the cc
					
					
//					//must finish up data in rolling buffer.
//					rolling.flip();	
//					
//					final ByteBuffer[] writeHolderUnWrap = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);
//					try {
//						unwrapRollingNominal(rolling, cc, writeHolderUnWrap, null);	
//					} catch (SSLException sslex) {
//						logger.warn("did we not release the new block write?");
//						manageException(sslex, cc);						
//					}
//					if(cc.localRunningBytesProduced>0) {
//						if (!PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_PLAIN_210)) {
//							throw new RuntimeException("already checked for space should not happen.");
//						}
//						PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204, cc.localRunningBytesProduced);
//						cc.localRunningBytesProduced = -1;
//						
//						//assert(longs match the one for cc)
//						PipeWriter.writeLong(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, cc.getId());
//						//    PipeReader.copyLong(source, target, ClientNetResponseSchema.MSG_RESPONSE_200_FIELD_CONNECTIONID_201, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210_FIELD_CONNECTIONID_201);
//						
//						PipeWriter.publishWrites(target);
//						
//					}
					
				}
				
				PipeWriter.publishEOF(target);
				PipeReader.releaseReadLock(source);
				return -1;
			}
			
			int msgIdx = PipeReader.getMsgIdx(source);
			if (msgIdx == NetPayloadSchema.MSG_DISCONNECT_203) {
				
				logger.info("UNWRAP FOUND DISCONNECT MESSAGE");
				
				PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_DISCONNECT_203);
				PipeWriter.writeLong(target, NetPayloadSchema.MSG_DISCONNECT_203_FIELD_CONNECTIONID_201, PipeReader.readLong(source, NetPayloadSchema.MSG_DISCONNECT_203_FIELD_CONNECTIONID_201));				
				PipeWriter.publishWrites(target);
				PipeReader.releaseReadLock(source);
				return didWork;
			}
			assert(msgIdx == NetPayloadSchema.MSG_ENCRYPTED_200);
	
			
			final ByteBuffer[] writeHolderUnWrap = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);

			//System.err.println("rolling holds "+rolling.position()+" and is of length "+rolling.capacity());
			
			SSLEngineResult result1 = gatherPipeDataForUnwrap(source, rolling, cc, writeHolderUnWrap, isServer);
			
			if (null==result1 && rolling.position()!=0) { //rolling has content to consume
				rolling.flip();	

				try {
					result1 = unwrapRollingNominal(rolling, cc, writeHolderUnWrap, result1);	
				} catch (SSLException sslex) {
					boolean isClient = null==handshakePipe;
					logger.warn("Error found on client {} ",isClient);
					manageException(sslex, cc, isServer);
					result1 = null;					
				}

			} 
			//else rolling has no data so nothing to do.
			
			PipeReader.releaseReadLock(source);
			
			
			SSLEngineResult result = result1;
			Status status = null==result?null:result.getStatus();			
			
			boolean needClose = false;
			if(cc.localRunningBytesProduced>0) {
				if (!PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_PLAIN_210)) {
					throw new RuntimeException("already checked for space should not happen.");
				}
				PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204, cc.localRunningBytesProduced);
				cc.localRunningBytesProduced = -1;
				
				//assert(longs match the one for cc)
				PipeWriter.writeLong(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201, cc.getId());
				//    PipeReader.copyLong(source, target, ClientNetResponseSchema.MSG_RESPONSE_200_FIELD_CONNECTIONID_201, ClientNetResponseSchema.MSG_SIMPLERESPONSE_210_FIELD_CONNECTIONID_201);
				
				PipeWriter.writeLong(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, /*(rolling.position()!=0 || Pipe.contentRemaining(source)!=0)?0:*/Pipe.tailPosition(source));//this is after release so should be next write position.
				
				PipeWriter.publishWrites(target);
				
				if (cameFromHandshake && Pipe.contentRemaining(source)==0 && rolling.position()==0) {
										
		        	sendAck(source, ack, cc);		//TODO: is this right, can the parser get a partial which it keeps backed by this pipe?	seems like yes.        	
					
				}
				
				
			} else {
				needClose = true;
			}
			
			if (status==Status.OK || status==null) {						
				if (needClose) {
					PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204, cc.localRunningBytesProduced);
				}
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
						
						
						PipeWriter.writeLong(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, /*(rolling.position()!=0 || Pipe.contentRemaining(source)!=0)?0:*/Pipe.tailPosition(source));//this is after release so should be next write position.
												
						PipeWriter.publishWrites(target);
						
						if (cameFromHandshake && Pipe.contentRemaining(source)==0 && rolling.position()==0) {
							
				        	sendAck(source, ack, cc);			        	
							
						}
					}
				}
			} else {				
//				assert(status == Status.BUFFER_OVERFLOW);
//				
//				throw new RuntimeException("server is untrustworthy? Output pipe is too small for the content to be written inside "+
//			                               target.maxAvgVarLen+" reading from "+rolling.position());
				
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
						
					
						PipeWriter.writeLong(target, NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206, /*(rolling.position()!=0 || Pipe.contentRemaining(source)!=0)?0:*/Pipe.tailPosition(source));//this is after release so should be next write position.
						
						
						PipeWriter.publishWrites(target);
						
						if (cameFromHandshake && Pipe.contentRemaining(source)==0 && rolling.position()==0) {
							
				        	sendAck(source, ack, cc);			        	
							
						}
					}
				}
				return 0;
				
			}	
		}
		
		return didWork;
		
	}

	private static void sendAck(Pipe<NetPayloadSchema> source, Pipe<NetParseAckSchema> ack, SSLConnection cc) {
		if (Pipe.hasRoomForWrite(ack)) {
			int s = Pipe.addMsgIdx(ack, NetParseAckSchema.MSG_PARSEACK_100);
			Pipe.addLongValue(cc.id,ack);			        		
			Pipe.addLongValue(Pipe.tailPosition(source),ack);			        		
			Pipe.confirmLowLevelWrite(ack, s);
			Pipe.publishWrites(ack);
		} else {
			logger.error("A server no room for ack of {} {}",cc.id,ack);
		}
	}

}
