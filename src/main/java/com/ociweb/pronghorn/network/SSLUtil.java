package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;

import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;

public class SSLUtil {

	private static final int SIZE_OF_PLAIN = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210);
	private static final int SIZE_OF_RELEASE_MSG = Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASEWITHSEQ_101);
	final static ByteBuffer noData = ByteBuffer.allocate(0);
	final static ByteBuffer[] noDatas = new ByteBuffer[]{noData,noData};
	private static final Logger logger = LoggerFactory.getLogger(SSLUtil.class);
	

    public static final long HANDSHAKE_TIMEOUT = 180_000_000_000L; // 120 sec, this is a very large timeout for handshake to complete.
    public static final long HANDSHAKE_POS = -123;
    
	
	public static boolean handShakeWrapIfNeeded(SSLConnection cc, Pipe<NetPayloadSchema> target, ByteBuffer buffer, boolean isServer, long arrivalTime) {
		//synchronized (cc.engine) {
				
		 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();
		 
	//	 System.err.println(cc.id+" handshake "+handshakeStatus);
		 
		 
		 boolean didShake = false;
		 while (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus && HandshakeStatus.FINISHED != handshakeStatus	 ) {
			 			 
			 didShake = true;
			 if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {				 
				 long nsDuration = cc.durationWaitingForNetwork();
				 if (nsDuration > HANDSHAKE_TIMEOUT) {
					 logger.warn("XXXXXXXXXXXXXXXXXXXXXXXXXXXx Handshake wrap abanonded for {} due to timeout of {} ns waiting for unwrap done by other stage.",cc,HANDSHAKE_TIMEOUT);
					 cc.close();				 
					 System.exit(-1);
				 }
				 return true;//done by the other stage.
			 }
			 
			 if (HandshakeStatus.NEED_WRAP == handshakeStatus) {				 
				 SSLUtil.handshakeWrapLogic(cc, target, buffer, isServer, arrivalTime);
				 handshakeStatus = cc.getEngine().getHandshakeStatus();				
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
		
		//  logger.info("server {} wrap status is now {} for id {} ",isServer,handshakeStatus, cc.getId());
		 
		 return didShake;//false
	  // }
	}
	
	public static void handshakeWrapLogic(SSLConnection cc, Pipe<NetPayloadSchema> target, ByteBuffer buffer, boolean isServer, long arrivalTime) {
	    
		try {
			
			//TODO: must guarantee room to complete before we start.
			
			do {
				
				//TODO: very odd hack here. for handshake, needs to be redesigned to drop this loop.
				int x=1_000;
				while (!Pipe.hasRoomForWrite(target)) {
					Thread.yield();
					if (--x<=0) {
						throw new UnsupportedOperationException("TOO LONG SPINNING");
					}
				}
				
				final ByteBuffer[] targetBuffers = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(target), target);
				final Status status = SSLUtil.wrapResultStatusState(target, buffer, cc, noDatas, targetBuffers, isServer, arrivalTime);
				
				if (Status.OK == status) {
					
					Pipe.confirmLowLevelWrite(target, Pipe.sizeOf(target, NetPayloadSchema.MSG_ENCRYPTED_200));
					Pipe.publishWrites(target);
					
				} else {
					//connection was closed before handshake completed 
				    if (Status.CLOSED == status) {
				    	cc.close();
				    	//already closed, NOTE we should release this from reserved pipe pools
				    //	logger.warn("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX unable to write due to coosed status, DO NOT EXPECT RESPOSNE.");
				    	//no need to cancel wrapped buffer it was already done by wrapResultStatusState
				    	return;
				    } else {
				    	logger.info("HANDSHAKE unable to wrap {} {} {} ",status, cc.getClass().getSimpleName(), cc.engine, new Exception());
						throw new RuntimeException();		
				    }
				}
			
				//TODO: can the wraps be one message on outgoign pipe??? yes as long as we have wrap requesets in a row, close on first non wrap request. faster as well.
				
				
			} while(cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_WRAP); //what about TODO: outgoing pipe will fill up?
			
					
		} catch (SSLException e) {
			logger.error("unable to wrap ", e);
			
			Pipe.unstoreBlobWorkingHeadPosition(target);
		}	
	    
	}

	static void manageException(SSLException sslex, SSLConnection cc, boolean isServer) {
		try {
			cc.close();
		} catch (Throwable t) {
			//ignore we are closing this connection
		}
		logger.error("Exception, in server:{} '{}'",isServer, sslex.getLocalizedMessage(), sslex);
	}

	public static Status wrapResultStatusState(Pipe<NetPayloadSchema> target, ByteBuffer buffer,
			final SSLConnection cc, ByteBuffer[] bbHolder, final ByteBuffer[] targetBuffers, boolean isServer, long arrivalTime) throws SSLException {
	
		if (cc.getEngine().isOutboundDone()) {
			
			Pipe.unstoreBlobWorkingHeadPosition(target);	
			return Status.CLOSED; 
		}
		
		SSLEngineResult result = cc.getEngine().wrap(bbHolder, targetBuffers[0]); //TODO: writing to room of 12264 from hoolder target should be 16K or so.., why buffer overflow when we have so much room?
		
		Status status = result.getStatus();

		//logger.trace("status state {} wrote out {} bytges for connection {} target room {} {} ",status,result.bytesProduced(),cc.getId(), targetBuffers[0].remaining(), targetBuffers[0]);
		
		if (status==Status.OK) {
					
			Pipe.addMsgIdx(target, NetPayloadSchema.MSG_ENCRYPTED_200);
			Pipe.addLongValue(cc.getId(), target);
			Pipe.addLongValue(arrivalTime, target);
			int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(target);
			Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int) result.bytesProduced(), target);

			
		} else if (status==Status.CLOSED){
							
			Pipe.unstoreBlobWorkingHeadPosition(target);
			try {
				 cc.getEngine().closeOutbound();				 
				 handShakeWrapIfNeeded(cc, target, buffer, isServer, arrivalTime);				 
				 cc.getSocketChannel().close();
				 cc.close();
			} catch (IOException e) {
				cc.close();
				ClientConnection.logger.warn("Error closing connection ",e);
			}				
			
		} else if (status==Status.BUFFER_OVERFLOW) {
		    assert(0==result.bytesProduced());

			///////////
			//This is only needed because engine.wrap does not take multiple target ByteBuffers as it should have.
			///////////
			try {
				buffer.clear();
				SSLEngineResult result2 = cc.getEngine().wrap(bbHolder, buffer);
				
				status = result2.getStatus();				
				
			//	logger.trace("overview processing status {} bytes produced {} for id {}",status,result2.bytesProduced(),cc.getId());
				
				
				if (status == Status.OK) {
					
					//write buffer to openA and openB
					buffer.flip();
					
					copyBufferIntoTwoBuffers(buffer, targetBuffers);
															
					Pipe.addMsgIdx(target, NetPayloadSchema.MSG_ENCRYPTED_200);
					Pipe.addLongValue(cc.getId(), target);	
					Pipe.addLongValue(arrivalTime, target);
					int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(target);
					Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int) result2.bytesProduced(), target);

				} else if (status == Status.BUFFER_OVERFLOW){
				
					logger.info("ZZZZZ Buffer overflow {} ",buffer);
					System.exit(-1);
					
				} else {
					
					Pipe.unstoreBlobWorkingHeadPosition(target);	
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
			
			new Exception("case should not happen, we have too little data to be wrapped and sent").printStackTrace();
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
		
		assert(rolling.limit()==rolling.capacity());
		
		int meta = Pipe.takeRingByteMetaData(source);
		int len = Pipe.takeRingByteLen(source);		
		ByteBuffer[] inputs =  Pipe.wrappedReadingBuffers(source, meta, len);
			
		assert(inputs[0].remaining()>0);
		
		cc.localRunningBytesProduced = 0;
		if (inputs[1].remaining()==0) {
			//if we have some rolling data from previously
			ByteBuffer firstPartOfBuffer = inputs[0];
			
			if (rolling.position()==0) {
		//		System.err.println(source.id+"A "+firstPartOfBuffer+"  "+inputs[1]);
				try {
					result = unwrap(cc, firstPartOfBuffer, targetBuffer);
				} catch (SSLException sslex) {
					manageException(sslex, cc, isServer);	//the connection is closed
					
					return null; //TODO: need to continue with new connections
					
				}				
				rolling.put(firstPartOfBuffer);
				assert(0==firstPartOfBuffer.remaining());
				
				
			} else {
				//add this new content onto the end before use.				
				rolling.put(firstPartOfBuffer);
				assert(0==firstPartOfBuffer.remaining());
			}
		} else {
			assert(inputs[0].hasRemaining());
			assert(inputs[1].hasRemaining());
			
			rolling.put(inputs[0]); 
			rolling.put(inputs[1]);  
	
		}
		return result;
	}

	private static SSLEngineResult unwrap(SSLConnection cc, ByteBuffer sourceBuffer, final ByteBuffer[] targetBuffer)
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

	/**
	 * Consume rolling which must be positioned for reading from position up to limit.
	 * Resturns rolling setup for appending new data so limit is at capacity and position is where we left off.		
	 */
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
				  //  assert(cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_UNWRAP) : "found "+cc.getEngine().getHandshakeStatus();
				
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
		} else {
			//logger.info("CLEAR");
			rolling.clear();
		}
		assert(rolling.limit()==rolling.capacity());
		
		
		return result;
	}
	
	private static SSLEngineResult unwrapRollingNominal(ByteBuffer rolling, SSLConnection cc, final ByteBuffer[] targetBuffer,	SSLEngineResult result) throws SSLException {
		int x=0;
		String rollingData = rolling.toString();
		while (rolling.hasRemaining()) {
				x++;
			
			    ////////////////////////
			    ///Block needed for openSSL limitation
			    ///////////////////////
			    int origLimit = rolling.limit();
			    int pos = rolling.position();
			    if (origLimit-pos>SSLEngineFactory.maxEncryptedContentLength) {
			    	rolling.limit(pos+SSLEngineFactory.maxEncryptedContentLength);
			    }
			    /////////////////////////
  
			    try {
			    	result = cc.getEngine().unwrap(rolling, targetBuffer);		
			    } catch (SSLException sslex) {
			    	sslex.printStackTrace();
			    	System.err.println("iteration "+x+" consumed "+rollingData+" for id"+cc.id+" on rolling ");
			    	System.exit(-1);
			    	
			    }
				rolling.limit(origLimit); //return origLimit, see above openSSL issue.
				
				int bytesProduced = result.bytesProduced();
				assert(cc.localRunningBytesProduced>=0);
				cc.localRunningBytesProduced += bytesProduced;
				
				if (result.getStatus() != Status.OK || cc.getEngine().getHandshakeStatus()!=HandshakeStatus.NOT_HANDSHAKING) {
					break;
				}
		}

		
		if (rolling.hasRemaining()) {
			rolling.compact(); //ready for append
		} else {
			//logger.info("CLEAR");
			rolling.clear();
		}
		assert(rolling.limit()==rolling.capacity());
		
		return result;
	}
	

	public static int handShakeUnWrapIfNeeded(final SSLConnection cc, final Pipe<NetPayloadSchema> source, ByteBuffer rolling, final ByteBuffer[] workspace, 
			                                      Pipe<NetPayloadSchema> handshakePipe, ByteBuffer secureBuffer, boolean isServer, long arrivalTime) {
		
		 assert(handshakePipe!=null);
		 assert(source!=null);  
		 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();	
		 int didWork = 0;
		 while (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus && HandshakeStatus.FINISHED != handshakeStatus) {
			 //logger.info("handshake {} {}",handshakeStatus,cc.getId());
		
			 if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
				handshakeWrapLogic(cc, handshakePipe, secureBuffer, isServer, arrivalTime);
				handshakeStatus = cc.getEngine().getHandshakeStatus();	
			 } else
			 
			 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
	                Runnable task;//TODO: there is anopporuntity to have this done by a different stage in the future.
	                while ((task = cc.getEngine().getDelegatedTask()) != null) {
	                	task.run();
	                }
	                handshakeStatus = cc.getEngine().getHandshakeStatus();
			 } else
			 
			 if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {
				// logger.info("server {} doing the unwrap now for {}  {}",isServer, cc.getId(), System.identityHashCode(cc));
			
				 	if (!Pipe.hasContentToRead(source)) {
				 		
				 		if (cc.durationWaitingForNetwork()>HANDSHAKE_TIMEOUT) {
				 			logger.info("No data provided for unwrap. timed out after {} ",HANDSHAKE_TIMEOUT);
				 			logger.info("XXXXXXXXXXXXXXXXXXXXXXXXXXXXxx no try read frament...................................",new Exception("how did we get here if there is nothing to read???"));
				 			System.exit(-1);
				 			cc.close();
				 		}
				 		
				 		if (rolling.position()>0) { 
				 			SSLEngineResult result;
							try { 
								rolling.flip();
								result = unwrapRollingHandshake(rolling, cc, workspace, null); //when done the wrapper is ready for writing more data to it
								//logger.info("server {} status is now {}  for {} ",isServer, cc.getEngine().getHandshakeStatus(),cc);
							} catch (SSLException sslex) {
								rolling.clear();
								logger.warn("UNWRAP|ERROR: needed handshake status of {} for server {}", cc.getEngine().getHandshakeStatus(), sslex, isServer,sslex);						
								manageException(sslex, cc, isServer);
								return -1;
							}
							if (null==result || Status.CLOSED == result.getStatus()) {
								 if (cc.getEngine().isOutboundDone()) {
									 	cc.close();
									 	logger.info("xxxxxxx was closed");
									 	return -1;
				                    } else {
				                        cc.getEngine().closeOutbound();
				                        handshakeStatus = cc.getEngine().getHandshakeStatus();
				                    }
							} else {					
								handshakeStatus = cc.getEngine().getHandshakeStatus();
							}
				 		
							if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {
								if (null!=result && Status.BUFFER_UNDERFLOW == result.getStatus() ) {
									return -1;//try again later when we have data								
								} else {									
									continue;
								}
							} else {
								continue;
							}
				 		} else {
				 			return -1;//try again later when we have data				 		
				 		}
				 		
				 		//throw new UnsupportedOperationException();
				 	}	
				 	//logger.info("have data for unwrap for {} ",cc.getId());
				 	
				 	cc.clearWaitingForNetwork();
				 	
				 	if (cc.id!=Pipe.peekLong(source, 1)) {
				 		return -1;//message not for this connection.
				 	}
				 	
				 	
				 //	int[] range = Arrays.copyOfRange(Pipe.slab(source), (int)Pipe.getWorkingTailPosition(source), (int)Pipe.getWorkingTailPosition(source)+100);
				 //	System.out.println(Arrays.toString(range));
				 	
				 	
				 	
				 	int msgId = Pipe.takeMsgIdx(source);
				 					 	
				    //if closed closd
	                if (msgId<0 || !cc.isValid) {	                	
	                	
	                	Pipe.skipNextFragment(source,msgId);
	          	                	
	                    if (cc.getEngine().isInboundDone() && cc.getEngine().isOutboundDone()) {
	                    	logger.info("quite wy");
	                        return -1;
	                    }	                    
	                    handshakeStatus = cc.closeInboundCloseOutbound(cc.getEngine());	  
	                    logger.info("check this case?? yes we need do a clean close "); //TODO: minor fix.
	                    return -1;
	                }
	             
	                
	                if (msgId == NetPayloadSchema.MSG_BEGIN_208) {
	                	
	                	
	                	throw new RuntimeException("not yet supported");
	                	
	                	
	                }
	                
	                
	                long connectionId = Pipe.takeLong(source);
	                assert(connectionId == cc.id) : "msg id "+msgId+" with connectionId "+connectionId+" oldcon id "+cc.id;
	                
	                
	                assert((msgId == NetPayloadSchema.MSG_PLAIN_210) || (msgId == NetPayloadSchema.MSG_ENCRYPTED_200));
	                arrivalTime = Pipe.takeLong(source);
	                
	                if (msgId == NetPayloadSchema.MSG_PLAIN_210) {
	                    long positionId = Pipe.takeLong(source);	
	                }
	                	                
					SSLEngineResult result = gatherPipeDataForUnwrap(source, rolling, cc, workspace, isServer);

					rolling.flip();	
					//logger.info("server {} unwrap rolling data {} for {} ", isServer, rolling, cc);
										
					
					try {
						result = unwrapRollingHandshake(rolling, cc, workspace, result);
						//logger.info("server {} status is now {}  for {} ",isServer, cc.getEngine().getHandshakeStatus(),cc);
					} catch (SSLException sslex) {
						rolling.clear();
						logger.warn("UNWRAP|ERROR: needed handshake status of {} for server {}", cc.getEngine().getHandshakeStatus(), sslex, isServer,sslex);						
						manageException(sslex, cc, isServer);
						return -1;
					} finally {
										
						Pipe.confirmLowLevelRead(source, Pipe.sizeOf(source, msgId));
						Pipe.releaseReadLock(source);
					}
					if (null==result || Status.CLOSED == result.getStatus()) {
						 if (cc.getEngine().isOutboundDone()) {
							 	cc.close();
							 	logger.info("xxxxxxx was closed");
							 	return -1;
		                    } else {
		                        cc.getEngine().closeOutbound();
		                        handshakeStatus = cc.getEngine().getHandshakeStatus();
		                    }
					} else {					
						handshakeStatus = cc.getEngine().getHandshakeStatus();
					}
		
					if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {
						if (null!=result && Status.BUFFER_UNDERFLOW == result.getStatus() ) {
							return -1;//try again later when we have data								
						} else {
							continue;
						}
					} else {
						continue;
					}
			 }
		 }

		 assert(handshakeStatus.equals(cc.getEngine().getHandshakeStatus()));
		 
		 
///		 logger.info("unwrap done with server {} handshake for {} needs {} did work {} rolling {} source {}", isServer, cc.id, cc.getEngine().getHandshakeStatus(), didWork, rolling, source);
		 return didWork;

	}

	/**
	 * Encrypt as much as possible based on the data available from the two pipes
	 */
	public static boolean engineWrap(SSLConnectionHolder ccm, Pipe<NetPayloadSchema> source, Pipe<NetPayloadSchema> target, ByteBuffer buffer, boolean isServer) {
		
		boolean didWork = false;
			
		while (Pipe.hasRoomForWrite(target) && Pipe.peekMsg(source, NetPayloadSchema.MSG_PLAIN_210) ) {
			didWork = true;
			
			final SSLConnection cc = ccm.get(Pipe.peekLong(source, 1));
						
			if (null==cc || !cc.isValid) {
				buffer.clear();
				//do not process this message because the connection has dropped				
				Pipe.skipNextFragment(source);
				continue;
			}
			
			buffer.clear(); //buffer will not bring in ANY data.

		//	logger.info("wrap data for {} ",cc.getId());
			
			if (handShakeWrapIfNeeded(cc, target, buffer, isServer, Pipe.peekLong(source, 3))) {
				
				//we know the message is plain but what was the position? if this is an empty message just for handshake then clear it
				long pos = Pipe.peekLong(source, 5);
				if (pos == HANDSHAKE_POS) {
					Pipe.skipNextFragment(source);
				}				
				
				return didWork;
			}
			
			int msgIdx = Pipe.takeMsgIdx(source);
			assert( NetPayloadSchema.MSG_PLAIN_210==msgIdx);
			
			long connectionId = Pipe.takeLong(source);
			long arrivalTime = Pipe.takeLong(source);
			
			assert(cc.id == connectionId);
			
			long positionId = Pipe.takeLong(source);
			
			
			int meta = Pipe.takeRingByteMetaData(source);
			int len = Pipe.takeRingByteLen(source);			
			ByteBuffer[] soruceBuffers = Pipe.wrappedReadingBuffers(source, meta, len);									
	
			try {
				final ByteBuffer[] targetBuffers = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(target), target);
				Status status = wrapResultStatusState(target, buffer, cc, soruceBuffers, targetBuffers, isServer, arrivalTime);
											
				if (status == Status.OK) {
					didWork = true;
					
					Pipe.confirmLowLevelWrite(target, Pipe.sizeOf(target, NetPayloadSchema.MSG_ENCRYPTED_200));
					Pipe.publishWrites(target);
													
					assert((soruceBuffers[0].remaining()==0) && (soruceBuffers[1].remaining()==0) ) :"the expected data was not consumed";
													
				} else if (status == Status.CLOSED) {	
				} else {					
					new Exception("XXXXX unexpected status "+status).printStackTrace();;
				}
			
			} catch (SSLException sslex) {
				manageException(sslex, cc, isServer);				
			} finally {
				Pipe.confirmLowLevelRead(source, SIZE_OF_PLAIN);
				Pipe.releaseReadLock(source);				
			}
			
		}
		buffer.clear();
		
		return didWork;
	}

	public static boolean validateStartMsgIdx(Pipe<NetPayloadSchema> source, boolean isServer) {
		if (Pipe.hasContentToRead(source)) {
			int[] starts = Pipe.from(source).messageStarts;
			int next = Pipe.peekInt(source);
			int i = starts.length;
			boolean found = false;
			while (--i>=0) {
				if (starts[i]==next) {
					found = true;
				}
			}
			if (!found) {
				logger.info("next message is "+Pipe.peekInt(source)+" on server "+isServer); //TODO: what is this number??

				int[] temp = Arrays.copyOfRange(Pipe.slab(source), (int)0, (int)Pipe.tailPosition(source)+10);
				
				logger.info("data "+Arrays.toString(temp));
				
				return false;
			}
		}
		return true;
	}

	
	//TODO: urgent unwrap sequence numbers are broken?
	//      do not know when to increement????
	//      do not know how share with client???
	
	public static int engineUnWrap(SSLConnectionHolder ccm, Pipe<NetPayloadSchema> source, Pipe<NetPayloadSchema> target, 
			                        ByteBuffer rolling, ByteBuffer[] workspace, Pipe<NetPayloadSchema> handshakePipe, Pipe<ReleaseSchema> releasePipe, 
			                        ByteBuffer secureBuffer, int groupId, boolean isServer) {
		
		///TODO: URGENT REWIRTE TO LOW LEVEL API SINCE LARGE SERVER CALLS VERY OFTEN.
		
		int didWork = 0;
		
		while (Pipe.hasContentToRead(source) ) {
			
			if (!Pipe.hasRoomForWrite(target)) {
				return didWork;//try again later when there is room in the output
			}			
				
			SSLConnection cc = null;
			long arrivalTime = 0;
			if ( Pipe.peekMsg(source, NetPayloadSchema.MSG_ENCRYPTED_200)) { 	
	
				assert(Pipe.peekInt(source) == NetPayloadSchema.MSG_ENCRYPTED_200);
				
				final long connectionId = Pipe.peekLong(source, 1);
				assert(connectionId>0) : "invalid connectionId read "+connectionId+" msgid "+Pipe.peekInt(source);
				
				
				cc = ccm.get(connectionId); //connection id	
				assert(cc.id==connectionId) : "returned wrong object";
	
				if (null==cc || !cc.isValid) {
					
					logger.info("sever {} ignored closed connection {} connectionId {}",isServer,cc,connectionId);
					//do not process this message because the connection has dropped

					Pipe.skipNextFragment(source);
					continue;
				}

				//need to come back in for needed wrap even without content to read but... we need the content to give use the CC !!!
				didWork = handShakeUnWrapIfNeeded(cc, source, rolling, workspace, handshakePipe, secureBuffer, isServer, arrivalTime=Pipe.peekLong(source, 3));
				assert(rolling.limit()==rolling.capacity());	
				
				if (didWork<0) {
					
				//	logger.info("still doing handshake");
					if (rolling.position()==0) {
						//send release because handshake is incomplete, waiting on other side or the connection has been closed
						sendRelease(source, releasePipe, cc, isServer);						
					}
					///////////
					return 0;// this case needs more data to finish handshake so returns
					///////////
					
				} else if (didWork==1) {	
				//	logger.info("finished shake");
					if (null!=releasePipe && rolling.position()==0 && Pipe.contentRemaining(source)==0) {						
						sendRelease(source, releasePipe, cc, isServer);
					}
					if (HandshakeStatus.NOT_HANDSHAKING !=  cc.getEngine().getHandshakeStatus()) {
						///////////////////
						return 1;//not yet done with handshake so do not start processing data.
						///////////////////
					}
				} else {
				//	logger.info("finished shake2");
					assert(HandshakeStatus.NOT_HANDSHAKING ==  cc.getEngine().getHandshakeStatus()) : "handshake status is "+cc.getEngine().getHandshakeStatus();
					//we can begin processing data now.
					
					
					//send begin here??
					
					assert(null!=cc);
					
				}
			} else {
				
				//this is EOF or the Begin message to be relayed
				if (Pipe.peekMsg(source, NetPayloadSchema.MSG_BEGIN_208)) {										
										
					Pipe.addMsgIdx(target, Pipe.takeMsgIdx(source));
					Pipe.addIntValue(Pipe.takeInt(source), target); // sequence
					
					Pipe.confirmLowLevelWrite(target, Pipe.sizeOf(target, NetPayloadSchema.MSG_BEGIN_208));
					Pipe.confirmLowLevelRead(source, Pipe.sizeOf(source, NetPayloadSchema.MSG_BEGIN_208));
					
					Pipe.publishWrites(target);
					Pipe.releaseReadLock(source);					
					
	//				logger.info("FINISHED BEGIN, NOW CONTINUE.."+source);
					
					//only need to release the read, the write has already been published to target
					continue;
				}
				//assert(null!=cc);
			}
			
			assert(rolling.limit()==rolling.capacity());
				
			SSLEngineResult result1 = null;
			ByteBuffer[] writeHolderUnWrap;
			
			if (Pipe.hasContentToRead(source)) {
				
				int msgIdx = Pipe.takeMsgIdx(source); 
				if (msgIdx<0) {	
					shutdownUnwrapper(source, target, rolling, isServer, cc, System.currentTimeMillis());
					return -1;
				} else if (msgIdx == NetPayloadSchema.MSG_DISCONNECT_203) {
					
					logger.info("UNWRAP FOUND DISCONNECT MESSAGE");										
					
					Pipe.addMsgIdx(target, NetPayloadSchema.MSG_DISCONNECT_203);
					Pipe.addLongValue(Pipe.takeLong(source), target); //ConnectionId
					Pipe.confirmLowLevelWrite(target,Pipe.sizeOf(target, NetPayloadSchema.MSG_DISCONNECT_203));
					Pipe.publishWrites(target);

					Pipe.confirmLowLevelRead(source, Pipe.sizeOf(source, msgIdx));
					Pipe.releaseReadLock(source);
													
					return didWork;
				} else if (msgIdx == NetPayloadSchema.MSG_BEGIN_208) {
					
					Pipe.addMsgIdx(target, msgIdx);
					Pipe.addIntValue(Pipe.takeInt(source), target); // sequence
					
					Pipe.confirmLowLevelWrite(target, Pipe.sizeOf(target, NetPayloadSchema.MSG_BEGIN_208));
					Pipe.confirmLowLevelRead(source, Pipe.sizeOf(source, NetPayloadSchema.MSG_BEGIN_208));
					
					Pipe.publishWrites(target);
					Pipe.releaseReadLock(source);	
					
					return didWork;
				} else {
					assert(msgIdx == NetPayloadSchema.MSG_ENCRYPTED_200) : "source provided message of "+msgIdx;					
				
					long connectionId = Pipe.takeLong(source);
					arrivalTime = Pipe.takeLong(source);
					assert(cc.id == connectionId);
				}
				
				
				//////////////		
				writeHolderUnWrap = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(target), target); //byte buffers to write payload

				result1 = gatherPipeDataForUnwrap(source, rolling, cc, writeHolderUnWrap, isServer);
											
				Pipe.confirmLowLevelRead(source, Pipe.sizeOf(source, msgIdx));
				Pipe.releaseReadLock(source);
				
			} else {				
				writeHolderUnWrap = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(target), target); //byte buffers to write payload
			}
			
			SSLEngineResult result = result1;
			Status status = null==result?null:result.getStatus();			
			
		//	logger.info("input status {}", status);
			
			if ((null==status || Status.OK==status) && rolling.position()>0) { //rolling has content to consume
				rolling.flip();	
				
				try {
					/////////////////
					//this method will consume all it can from rolling before returning
					//no need to worry about remaining data in rolling, it must be a partial waiting on extra data
					////////////////
					result = unwrapRollingNominal(rolling, cc, writeHolderUnWrap, result); //remaining data is ready for append	
					status = null==result?null:result.getStatus();	
				} catch (SSLException sslex) {
					rolling.clear();//TODO: consume all the broken messages...
					manageException(sslex, cc, isServer);
					result1 = null;
					continue;
				}
				
			} 				
						
			//else rolling has no data so nothing to do.
			assert(rolling.limit()==rolling.capacity());			
			
			publishWrittenPayloadForUnwrap(source, target, rolling, releasePipe, cc, arrivalTime);
					
		//	System.err.println("rolling for id "+cc.id+" holds "+rolling+" "+cc.getEngine().getHandshakeStatus()+"  "+cc.getEngine().getSession()+"  "+status);
			
			//nothing need be done for OK or null.
			//nothing need be done for underflow, next read will get more data.
	         if (status == Status.BUFFER_OVERFLOW) {	
				//too much data and the buffer is not big enough
				//this should not happen.
				logger.info("OVERFLOW, the pipe {} is not configured to be large enough.", target.id);				
				return 0;
				
			} else if (status==Status.CLOSED){				
				logger.info("closed status detected");				
				try {
					 cc.getEngine().closeOutbound();
					 handShakeUnWrapIfNeeded(cc, source, rolling, workspace, handshakePipe, secureBuffer, isServer, arrivalTime);
				     cc.getSocketChannel().close();
				} catch (IOException e) {
					cc.isValid = false;
					ClientConnection.logger.warn("Error closing connection ",e);
				}				
				//clear the rolling for the next user/call since this one is closed
				rolling.clear();
				cc.close();
				
			}
		}
		
		return didWork;
		
	}

	private static void publishWrittenPayloadForUnwrap(Pipe<NetPayloadSchema> source, Pipe<NetPayloadSchema> target,
			ByteBuffer rolling, Pipe<ReleaseSchema> releasePipe, SSLConnection cc, long arrivalTime) {
	
		if(cc.localRunningBytesProduced>0) {
			int size = Pipe.addMsgIdx(target, NetPayloadSchema.MSG_PLAIN_210);
			Pipe.addLongValue(cc.getId(), target); //connection id	
			Pipe.addLongValue(arrivalTime, target);
			long releasePosition = rolling.hasRemaining()? 0 : Pipe.tailPosition(source);
			Pipe.addLongValue(releasePosition, target); //position
			
			int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(target);
			Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int)cc.localRunningBytesProduced, target);

			cc.localRunningBytesProduced = -1;

			Pipe.confirmLowLevelWrite(target, size);
			Pipe.publishWrites(target);			
		} else {
			Pipe.unstoreBlobWorkingHeadPosition(target);
			
		}
	}

	private static void shutdownUnwrapper(Pipe<NetPayloadSchema> source, Pipe<NetPayloadSchema> target,
			ByteBuffer rolling, boolean isServer, SSLConnection cc, long arrivalTime) {
		if (rolling.position()>0 && null!=cc) {
			logger.info("shutdown of unwrap detected but we must procesing rolling data first {} isServer:{}",rolling,isServer);

			//Must send disconnect first to empty the data when we know the cc
			
			
			//must finish up consuming data in rolling buffer.
			rolling.flip();	
					
			
			final ByteBuffer[] writeHolderUnWrap = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(target), target); //byte buffers to write payload

			try {
				unwrapRollingNominal(rolling, cc, writeHolderUnWrap, null);	
			} catch (SSLException sslex) {
				logger.warn("did we not release the new block write?");
				manageException(sslex, cc, isServer);						
			}
			if(cc.localRunningBytesProduced>0) {
				int size = Pipe.addMsgIdx(target, NetPayloadSchema.MSG_PLAIN_210);
				Pipe.addLongValue(cc.getId(), target); //connection id	
				Pipe.addLongValue(arrivalTime, target);
				long releasePosition = rolling.hasRemaining()? 0 : Pipe.tailPosition(source);
				Pipe.addLongValue(releasePosition, target); //position
				
				int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(target);
				Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int)cc.localRunningBytesProduced, target);
				cc.localRunningBytesProduced = -1;
		
				Pipe.confirmLowLevelWrite(target, size);
				Pipe.publishWrites(target);				
			} else {
				Pipe.unstoreBlobWorkingHeadPosition(target); //TODO: still under test.
				
			}
			
		}
		
		Pipe.publishEOF(target);
			
		Pipe.confirmLowLevelRead(source, Pipe.EOF_SIZE);
		Pipe.releaseReadLock(source);
	}

	private static void sendRelease(Pipe<NetPayloadSchema> source, Pipe<ReleaseSchema> release, SSLConnection cc, boolean isServer) {
		if (Pipe.hasRoomForWrite(release, SIZE_OF_RELEASE_MSG)) {
			sendReleaseRec(source, release, cc, cc.getSequenceNo(), isServer);
		} else {
			logger.error("Warning release pipes are too short for volume, if messsage is not added the system may hang {} {}",cc.id,release);
			//spin lock is required here to ensure this message is not lost, only happens in this case where the sytem is not configured for the right volume
			Pipe.spinBlockForRoom(release, SIZE_OF_RELEASE_MSG);
			//the above spin may stop early if the system is in the progress of shutting down
			if (Pipe.hasRoomForWrite(release, SIZE_OF_RELEASE_MSG)) {
				sendReleaseRec(source, release, cc, cc.getSequenceNo(), isServer);
			}
		}
	}

	private static void sendReleaseRec(Pipe<NetPayloadSchema> source, Pipe<ReleaseSchema> release, SSLConnection cc, int sequence, boolean isServer) {
		int s = Pipe.addMsgIdx(release, isServer? ReleaseSchema.MSG_RELEASEWITHSEQ_101 : ReleaseSchema.MSG_RELEASE_100);
		Pipe.addLongValue(cc.id,release);			        		
		Pipe.addLongValue(Pipe.tailPosition(source),release);
		if (isServer) {
			Pipe.addIntValue(sequence, release);
		}
		Pipe.confirmLowLevelWrite(release, s);
		Pipe.publishWrites(release);
	}

}
