package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;

public class SSLUtil {

	private static final int SIZE_OF_PLAIN = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210);
	private static final int SIZE_OF_RELEASE_MSG = Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASEWITHSEQ_101);
	final static ByteBuffer noData = ByteBuffer.allocate(0);
	final static ByteBuffer[] noDatas = new ByteBuffer[]{noData,noData};
	private static final Logger logger = LoggerFactory.getLogger(SSLUtil.class);
	

    public static final long HANDSHAKE_TIMEOUT = 180_000_000_000L; // 120 sec, this is a very large timeout for handshake to complete.
    public static final long HANDSHAKE_POS = -123;
    
	
	public static boolean handShakeWrapIfNeeded(BaseConnection cc, Pipe<NetPayloadSchema> target, ByteBuffer buffer, boolean isServer, long arrivalTime) {
						
		 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();

		 boolean didShake = false;
		 while (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus && HandshakeStatus.FINISHED != handshakeStatus	 ) {

			 didShake = true;
			 if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {				 
				 if (cc.durationWaitingForNetwork() > HANDSHAKE_TIMEOUT) {
					
					 logger.warn("Handshake wrap abanonded for {} due to timeout of {} ms waiting for unwrap done by reading stage.",cc,HANDSHAKE_TIMEOUT/1000000);
					 cc.close();	
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
	                
	            //return (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus) && (HandshakeStatus.FINISHED != handshakeStatus);
			 }
		 }
		 cc.clearWaitingForNetwork();
		
		 // logger.info("server {} wrap status is now {} for id {} ",isServer,handshakeStatus, cc.getId());
		 
		 return didShake;
	 
	}
	
	public static void handshakeWrapLogic(BaseConnection cc, Pipe<NetPayloadSchema> target, ByteBuffer buffer, boolean isServer, long arrivalTime) {
	    
		try {

			do {
				if (!Pipe.hasRoomForWrite(target)) {
					return; //unable to complete, try again later
				}
				
				final ByteBuffer[] targetBuffers = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(target), target);
				final Status status = SSLUtil.wrapResultStatusState(target, buffer, cc, noDatas, targetBuffers, isServer, arrivalTime);
				
				if (Status.OK == status) {
					
					Pipe.confirmLowLevelWrite(target, Pipe.sizeOf(target, NetPayloadSchema.MSG_ENCRYPTED_200));
					Pipe.publishWrites(target);
					
				} else {
					//connection was closed before handshake completed 
					//already closed, NOTE we should release this from reserved pipe pools
					//no need to cancel wrapped buffer it was already done by wrapResultStatusState
					cc.close();
				    if (Status.CLOSED != status) {
				    	//not expected case so log this
				    	logger.warn("HANDSHAKE unable to wrap {} {} {} ",status, cc.getClass().getSimpleName(), cc.getEngine(), new Exception());	
				    }
				    return;
				}
			} while(cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_WRAP); 
			
					
		} catch (SSLException e) {
			//logger.error("unable to wrap ", e);
			
			Pipe.unstoreBlobWorkingHeadPosition(target);
		}	
	    
	}

	static void manageException(SSLException sslex, BaseConnection cc, boolean isServer) {
		try {
			cc.close();
		} catch (Throwable t) {
			//ignore we are closing this connection
		}
		logger.error("Exception, in SERVER:{} '{}'",isServer, sslex.getLocalizedMessage(), sslex);
	}

	public static Status wrapResultStatusState(Pipe<NetPayloadSchema> target, ByteBuffer buffer,
			final BaseConnection cc, ByteBuffer[] bbHolder, final ByteBuffer[] targetBuffers, boolean isServer, long arrivalTime) throws SSLException {
	
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
				logger.warn("Error closing connection ",e);
			}				
			
		} else if (status==Status.BUFFER_OVERFLOW) {
		    assert(0==result.bytesProduced());

			///////////
			//This is only needed because engine.wrap does not take multiple target ByteBuffers as it should have.
			///////////
			try {
				((Buffer)buffer).clear();
				SSLEngineResult result2 = cc.getEngine().wrap(bbHolder, buffer);
				
				status = result2.getStatus();				
				
			//	logger.trace("overview processing status {} bytes produced {} for id {}",status,result2.bytesProduced(),cc.getId());
				
				
				if (status == Status.OK) {
					
					//write buffer to openA and openB
					((Buffer)buffer).flip();
					
					copyBufferIntoTwoBuffers(buffer, targetBuffers);
															
					Pipe.addMsgIdx(target, NetPayloadSchema.MSG_ENCRYPTED_200);
					Pipe.addLongValue(cc.getId(), target);	
					Pipe.addLongValue(arrivalTime, target);
					int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(target);
					Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int) result2.bytesProduced(), target);

				} else if (status == Status.BUFFER_OVERFLOW){
				
					logger.warn("wrapResultStatusState Buffer overflow {} ",buffer);
					return status;
					
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
			((Buffer)buffer).limit(room);
		}										
		targetBuffers[0].put(buffer);
		((Buffer)buffer).limit(finalLimit);
		if (buffer.hasRemaining()) {
			targetBuffers[1].put(buffer);
		}
		assert(!buffer.hasRemaining());
	}

	private static void gatherPipeDataForUnwrap(int maxEncryptedContentLength, ByteBuffer rolling, BaseConnection cc, final ByteBuffer[] targetBuffer, boolean isServer, Pipe<NetPayloadSchema> source) {

		assert(rolling.limit()==rolling.capacity());
		
		int meta = Pipe.takeByteArrayMetaData(source);
		int len = Pipe.takeByteArrayLength(source);		
		ByteBuffer[] inputs =  Pipe.wrappedReadingBuffers(source, meta, len);

		assert(inputs[0].remaining()>0);
		
		cc.localRunningBytesProduced = 0;
		if (inputs[1].remaining()==0) {
			rolling.put(inputs[0]);
			assert(0==inputs[0].remaining());
		} else {			
			assert(inputs[0].hasRemaining());
			assert(inputs[1].hasRemaining());
			
			rolling.put(inputs[0]); 
			rolling.put(inputs[1]);  
	
		}
		
	}

	private static SSLEngineResult unwrap(int maxEncryptedContentLength, ByteBuffer sourceBuffer, final ByteBuffer[] targetBuffer, BaseConnection cc)
			throws SSLException {
		SSLEngineResult result;
		int origLimit;
		do {
			///////////////
			//Block needed for limitations of OpenSSL, can only  support small blocks to be decryptded at a time
			///////////////
			origLimit = sourceBuffer.limit();
			int pos = sourceBuffer.position();
			if (origLimit-pos>maxEncryptedContentLength) {
				sourceBuffer.limit(pos+maxEncryptedContentLength);
			}
			/////////////
			
			assert(sourceBuffer.remaining()<=maxEncryptedContentLength);

			result = cc.getEngine().unwrap(sourceBuffer, targetBuffer);//common case where we can unwrap directly from the pipe.

			((Buffer)sourceBuffer).limit(origLimit);//restore the limit so we can keep the remaining data (only critical for openSSL compatibility, see above)
			assert(cc.localRunningBytesProduced>=0);
			cc.localRunningBytesProduced += result.bytesProduced();
			
		
		} while(result.getStatus() == Status.OK && sourceBuffer.hasRemaining() && cc.getEngine().getHandshakeStatus()==HandshakeStatus.NOT_HANDSHAKING);
		return result;
	}

	/**
	 * Consume rolling which must be positioned for reading from position up to limit.
	 * Resturns rolling setup for appending new data so limit is at capacity and position is where we left off.		
	 */
	private static SSLEngineResult unwrapRollingHandshake(ByteBuffer rolling, int maxEncryptedContentLength, final ByteBuffer[] targetBuffer, BaseConnection cc) throws SSLException {
		
		SSLEngineResult result = null;
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
				
				while (cc.getEngine().getHandshakeStatus() == HandshakeStatus.NEED_UNWRAP 
			    		  && (result==null || result.getStatus() == Status.OK) 
			    		  && rolling.hasRemaining()) {
				//	    ////////////////////////
					    ///Block needed for openSSL limitation
					    ///////////////////////
					    int origLimit = rolling.limit();
					    int pos = rolling.position();
					    if (origLimit-pos>maxEncryptedContentLength) {
					    	rolling.limit(pos+maxEncryptedContentLength);
					    }
					    /////////////////////////
						result = cc.getEngine().unwrap(rolling, targetBuffer);			
		
						//	System.err.println("C unwrapped bytes produced "+result.bytesProduced()+"  "+result.getStatus()+"  "+"  "+rolling.hasRemaining()+" "+cc.getEngine().getHandshakeStatus() );
						
						((Buffer)rolling).limit(origLimit); //return origLimit, see above openSSL issue.
						
						int bytesProduced = result.bytesProduced();
						assert(cc.localRunningBytesProduced>=0);
						cc.localRunningBytesProduced += bytesProduced;
				    }; //may need data
	
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
			((Buffer)rolling).clear();
		}
		assert(rolling.limit()==rolling.capacity());
		
		
		return result;
	}
	
	private static SSLEngineResult unwrapRollingNominal(ByteBuffer rolling, int maxEncryptedContentLength, final ByteBuffer[] targetBuffer, SSLEngineResult result, BaseConnection cc) throws SSLException {
		int x=0;
		String rollingData = rolling.toString();
		while (rolling.hasRemaining()) {
				x++;
			
			    ////////////////////////
			    ///Block needed for openSSL limitation
			    ///////////////////////
			    int origLimit = rolling.limit();
			    int pos = rolling.position();
			    if (origLimit-pos>maxEncryptedContentLength) {
			    	rolling.limit(pos+maxEncryptedContentLength);
			    }
			    /////////////////////////
  
			    try {
			    	result = cc.getEngine().unwrap(rolling, targetBuffer);		
			    } catch (SSLException sslex) {
			    	//logger.warn("TLS ",sslex);
			    	((Buffer)rolling).clear();
			    	cc.close();
			    	break;			    	
			    }
				((Buffer)rolling).limit(origLimit); //return origLimit, see above openSSL issue.
				
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
			((Buffer)rolling).clear();
		}
		assert(rolling.limit()==rolling.capacity());
		
		return result;
	}
	

	public static int handShakeUnWrapIfNeeded(int maxEncryptedContentLength, final Pipe<NetPayloadSchema> source, ByteBuffer rolling, final ByteBuffer[] workspace, Pipe<NetPayloadSchema> handshakePipe, ByteBuffer secureBuffer, boolean isServer, long arrivalTime, final BaseConnection cc) {
		
		 assert(handshakePipe!=null);
		 assert(source!=null);  
		 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();	
		 int didWork = 0;
		 while (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus && HandshakeStatus.FINISHED != handshakeStatus) {
			// logger.info("unwrap handshake {} {}",handshakeStatus,cc.getId());
		
			 if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
				handshakeWrapLogic(cc, handshakePipe, secureBuffer, isServer, arrivalTime);
				handshakeStatus = cc.getEngine().getHandshakeStatus();
				
			 } else if (HandshakeStatus.NEED_TASK == handshakeStatus) {
	                Runnable task;//TODO: there is an opporuntity to have this done by a different stage in the future.
	                while ((task = cc.getEngine().getDelegatedTask()) != null) {
	                	task.run();
	                }
	                handshakeStatus = cc.getEngine().getHandshakeStatus();
			 } else if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {
				    //logger.trace("server {} doing the unwrap now for {}  {}",isServer, cc.getId(), System.identityHashCode(cc));
			
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
								result = unwrapRollingHandshake(rolling, maxEncryptedContentLength, workspace, cc); //when done the wrapper is ready for writing more data to it
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
									 	//logger.info("xxxxxxx was closed");
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
	                if (msgId < 0 || !cc.isValid) {	                	
	                	
	                	Pipe.skipNextFragment(source,msgId);
	          	                	
	                    if (cc.getEngine().isInboundDone() && cc.getEngine().isOutboundDone()) {
	                    	return -1;
	                    }	                    
	                    handshakeStatus = cc.closeInboundCloseOutbound(cc.getEngine());	  
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
	                	                
	                gatherPipeDataForUnwrap(maxEncryptedContentLength, rolling, cc, workspace, isServer, source);

	                int tp = rolling.position();
	                int tl = rolling.limit();
					((Buffer)rolling).flip();	
					//logger.trace("server {} unwrap rolling data {} for {} ", isServer, rolling, cc);
					
					
					SSLEngineResult result;
					try {
						result = unwrapRollingHandshake(rolling, maxEncryptedContentLength, workspace, cc);
						//logger.info("server {} status is now {}  for {} ",isServer, cc.getEngine().getHandshakeStatus(),cc);
					} catch (SSLException sslex) {						
						//restore the rolling data since we could not process what we have so far 
						rolling.limit(tl);
						rolling.position(tp);
						//try again later
						return -1;
					} finally {
										
						Pipe.confirmLowLevelRead(source, Pipe.sizeOf(source, msgId));
						Pipe.releaseReadLock(source);
					}
					if (null==result || Status.CLOSED == result.getStatus()) {
						 if (cc.getEngine().isOutboundDone()) {
							 	cc.close();
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
		 
		 //logger.trace("unwrap done with server {} handshake for {} needs {} did work {} rolling {} source {}", isServer, cc.id, cc.getEngine().getHandshakeStatus(), didWork, rolling, source);
		 return didWork;

	}

	/**
	 * Encrypt as much as possible based on the data available from the two pipes
	 */
	public static boolean engineWrap(SSLConnectionHolder ccm, Pipe<NetPayloadSchema> source, Pipe<NetPayloadSchema> target, ByteBuffer buffer, boolean isServer) {
		
		boolean didWork = false;
			
		while (Pipe.hasRoomForWrite(target) && Pipe.peekMsg(source, NetPayloadSchema.MSG_PLAIN_210) ) {
			didWork = true;
			
			long connectionId = Pipe.peekLong(source, 0xFF&NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201);
			//TODO: what if the connection changes while in flight.
			final BaseConnection cc = ccm.lookupConnectionById(connectionId);
						
			if (null==cc || !cc.isValid) {
				//logger.info("connection has dropped and data with it");
				((Buffer)buffer).clear();
//				//do not process this message because the connection has dropped				
				Pipe.skipNextFragment(source);

				//send to target disconnect
				int size = Pipe.addMsgIdx(target, NetPayloadSchema.MSG_DISCONNECT_203);
				Pipe.addLongValue(connectionId, target);
				Pipe.confirmLowLevelWrite(target);
				Pipe.publishWrites(target);
				continue;
			}
			
			((Buffer)buffer).clear(); //buffer will not bring in ANY data.

		//	logger.info("wrap data for {} ",cc.getId());
			
			if (handShakeWrapIfNeeded(cc, target, buffer, isServer, Pipe.peekLong(source, 0xFFF&NetPayloadSchema.MSG_PLAIN_210_FIELD_ARRIVALTIME_210))) {
			
				//we know the message is plain but what was the position? if this is an empty message just for handshake then clear it
				if (Pipe.peekLong(source, 0xFFFF&NetPayloadSchema.MSG_PLAIN_210_FIELD_POSITION_206) == HANDSHAKE_POS) {
					//only skip this if we no longer need it for further checks.					
					Pipe.skipNextFragment(source);
					return didWork;
				}
			}
			//logger.trace("handshake not needed now continue sending data");
			
			int msgIdx = Pipe.takeMsgIdx(source);
			assert( NetPayloadSchema.MSG_PLAIN_210==msgIdx);
			
			Pipe.takeLong(source); //Connection Id
			long arrivalTime = Pipe.takeLong(source);
			
			long positionId = Pipe.takeLong(source);
			
			
			int meta = Pipe.takeByteArrayMetaData(source);
			int len = Pipe.takeByteArrayLength(source);			
			ByteBuffer[] soruceBuffers = Pipe.wrappedReadingBuffers(source, meta, len);									
	
			try {
				final ByteBuffer[] targetBuffers = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(target), target);
				Status status = wrapResultStatusState(target, buffer, cc, soruceBuffers, targetBuffers, isServer, arrivalTime);
											
				if (status == Status.OK) {
					didWork = true;
					
					Pipe.confirmLowLevelWrite(target, Pipe.sizeOf(target, NetPayloadSchema.MSG_ENCRYPTED_200));
					Pipe.publishWrites(target);
			
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
		((Buffer)buffer).clear();
		
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

	
	public static int engineUnWrap(SSLConnectionHolder ccm, Pipe<NetPayloadSchema> source, Pipe<NetPayloadSchema> target,
			                        ByteBuffer rolling, ByteBuffer[] workspace, Pipe<NetPayloadSchema> handshakePipe, Pipe<ReleaseSchema> releasePipe, 
			                        ByteBuffer secureBuffer, boolean isServer) {
		///TODO: URGENT REWIRTE TO LOW LEVEL API SINCE LARGE SERVER CALLS VERY OFTEN.

		int maxEncryptedContentLength = ccm.engineFactory.maxEncryptedContentLength();
		int didWork = 0;

		while (Pipe.hasContentToRead(source) ) {			
			if (Pipe.hasRoomForWrite(target)) {					

				BaseConnection cc = null;
				long arrivalTime = 0;
				if ( Pipe.peekMsg(source, NetPayloadSchema.MSG_ENCRYPTED_200)) { 	
					
					assert(Pipe.peekInt(source) == NetPayloadSchema.MSG_ENCRYPTED_200);
					
					final long connectionId = Pipe.peekLong(source, 1);
					assert(connectionId>0) : "invalid connectionId read "+connectionId+" msgid "+Pipe.peekInt(source);
					
					cc = ccm.lookupConnectionById(connectionId); //connection id	
	
					if (null==cc || !cc.isValid ) {
						Pipe.skipNextFragment(source);
						continue;
					}
	
					//need to come back in for needed wrap even without content to read but... we need the content to give use the CC !!!
					didWork = handShakeUnWrapIfNeeded(maxEncryptedContentLength, source, rolling, workspace, handshakePipe, secureBuffer, isServer, arrivalTime=Pipe.peekLong(source, 3), cc);
					assert(rolling.limit()==rolling.capacity());	
					
					if (didWork<0) {
						
						if (rolling.position()==0 ) {
							 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();	
							 if (HandshakeStatus.NEED_UNWRAP != handshakeStatus) {
								 //NOTE: Test to see if this avoids deadlocks: System.out.println("release: "+handshakeStatus);
								 //send release because handshake is incomplete, waiting on other side or the connection has been closed
								 sendRelease(source, releasePipe, cc, isServer);						
							 }
						}
						///////////
						return 0;// this case needs more data to finish handshake so returns
						///////////
						
					} else if (didWork==1) {	
						//logger.trace("finished shake");
						if (null!=releasePipe && rolling.position()==0 && Pipe.contentRemaining(source)==0) {		
							 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();	
							 if (HandshakeStatus.NEED_UNWRAP != handshakeStatus) {
								 //NOTE: Test to see if this avoids deadlocks: System.out.println("release: "+handshakeStatus);
								 sendRelease(source, releasePipe, cc, isServer);
							 }
						}
						if (HandshakeStatus.NOT_HANDSHAKING !=  cc.getEngine().getHandshakeStatus()) {
							///////////////////
							return 1;//not yet done with handshake so do not start processing data.
							///////////////////
						}
					} else {
						//logger.trace("finished shake2");
						assert(HandshakeStatus.NOT_HANDSHAKING ==  cc.getEngine().getHandshakeStatus()) : "handshake status is "+cc.getEngine().getHandshakeStatus();
						//we can begin processing data now.
						
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
			
						//only need to release the read, the write has already been published to target
						continue;
					}
	
				}
				
				assert(rolling.limit()==rolling.capacity());
					
				ByteBuffer[] writeHolderUnWrap;
				
				if (Pipe.hasContentToRead(source)) {
					
					int msgIdx = Pipe.takeMsgIdx(source); 
	
					if (msgIdx<0) {	
						shutdownUnwrapper(source, target, rolling, isServer, maxEncryptedContentLength, System.currentTimeMillis(), cc);
						return -1;
					} else if (msgIdx == NetPayloadSchema.MSG_DISCONNECT_203) {
	
						Pipe.addMsgIdx(target, NetPayloadSchema.MSG_DISCONNECT_203);
						long conId = Pipe.takeLong(source);
						Pipe.addLongValue(conId, target); //ConnectionId
						Pipe.confirmLowLevelWrite(target,Pipe.sizeOf(target, NetPayloadSchema.MSG_DISCONNECT_203));
						Pipe.publishWrites(target);
	
						Pipe.confirmLowLevelRead(source, Pipe.sizeOf(source, msgIdx));
						Pipe.releaseReadLock(source);
		
						rolling.clear();
						 
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
	
					gatherPipeDataForUnwrap(maxEncryptedContentLength, rolling, cc, writeHolderUnWrap, isServer, source);
												
					Pipe.confirmLowLevelRead(source, Pipe.sizeOf(source, msgIdx));
					Pipe.releaseReadLock(source);
					
				} else {				
					writeHolderUnWrap = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(target), target); //byte buffers to write payload
				}
				
				SSLEngineResult result=null;
				Status status = null==result?null:result.getStatus();			
		
				if ((null==status || Status.OK==status) && rolling.position()>0) { //rolling has content to consume
					((Buffer)rolling).flip();	
					
					try {
						/////////////////
						//this method will consume all it can from rolling before returning
						//no need to worry about remaining data in rolling, it must be a partial waiting on extra data
						////////////////
						result = unwrapRollingNominal(rolling, maxEncryptedContentLength, writeHolderUnWrap, result, cc); //remaining data is ready for append
						status = null==result?null:result.getStatus();	
					} catch (SSLException sslex) {
						rolling.clear();//TODO: consume all the broken messages...
						manageException(sslex, cc, isServer);
						continue;
					}
					
				} 				
							
				//else rolling has no data so nothing to do.
				assert(rolling.limit()==rolling.capacity());			
	
				publishWrittenPayloadForUnwrap(source, target, rolling, cc, arrivalTime);
						
			//	System.err.println("rolling for id "+cc.id+" holds "+rolling+" "+cc.getEngine().getHandshakeStatus()+"  "+cc.getEngine().getSession()+"  "+status);
				
				//nothing need be done for OK or null.
				//nothing need be done for underflow, next read will get more data.
		         if (status == Status.BUFFER_OVERFLOW) {	
					//too much data and the buffer is not big enough
					//this should not happen.
					logger.info("\nOVERFLOW, the pipe {} is not configured to be large enough. {} Connection has been closed.", target.id, target.config());	
					//((Buffer)rolling).clear();
					//cc.close();
					return 0;
					
				} else if (status==Status.CLOSED){				
					//logger.trace("closed status detected");				
					try {
						 cc.getEngine().closeOutbound();
						 handShakeUnWrapIfNeeded(maxEncryptedContentLength, source, rolling, workspace, handshakePipe, secureBuffer, isServer, arrivalTime, cc);
					     cc.getSocketChannel().close();
					} catch (IOException e) {
						cc.isValid = false;
						logger.warn("Error closing connection ",e);
					}				
					//clear the rolling for the next user/call since this one is closed
					((Buffer)rolling).clear();
					cc.close();
					
				}
			} else {
				return didWork;//try again later when there is room in the output
			}	         
		}
		
		return didWork;
		
	}

	private static void publishWrittenPayloadForUnwrap(Pipe<NetPayloadSchema> source, Pipe<NetPayloadSchema> target, ByteBuffer rolling, BaseConnection cc, long arrivalTime) {
			
		if(cc.localRunningBytesProduced>0) {
			int size = Pipe.addMsgIdx(target, NetPayloadSchema.MSG_PLAIN_210);
			Pipe.addLongValue(cc.getId(), target); //connection id	
			Pipe.addLongValue(arrivalTime, target);
			Pipe.addLongValue(Pipe.tailPosition(source), target); //position
			
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
										  ByteBuffer rolling, boolean isServer, int maxEncryptedContentLength, long arrivalTime, BaseConnection cc) {
		if (rolling.position()>0 && null!=cc) {
			logger.info("shutdown of unwrap detected but we must procesing rolling data first {} isServer:{}",rolling,isServer);

			//Must send disconnect first to empty the data when we know the cc
			
			
			//must finish up consuming data in rolling buffer.
			rolling.flip();	
					
			
			final ByteBuffer[] writeHolderUnWrap = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(target), target); //byte buffers to write payload

			try {
				unwrapRollingNominal(rolling, maxEncryptedContentLength, writeHolderUnWrap, null, cc);
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

	private static void sendRelease(Pipe<NetPayloadSchema> source, Pipe<ReleaseSchema> release, BaseConnection cc, boolean isServer) {
		Pipe.presumeRoomForWrite(release);
		sendReleaseRec(source, release, cc, cc.getSequenceNo(), isServer);
		
	}

	private static void sendReleaseRec(Pipe<NetPayloadSchema> source, Pipe<ReleaseSchema> release, BaseConnection cc, int sequence, boolean isServer) {
		int s = Pipe.addMsgIdx(release, isServer? ReleaseSchema.MSG_RELEASEWITHSEQ_101 : ReleaseSchema.MSG_RELEASE_100);
		Pipe.addLongValue(cc.id,release);			        		
		Pipe.addLongValue(Pipe.tailPosition(source),release);
		if (isServer) {
			Pipe.addIntValue(sequence, release);
		}
		Pipe.confirmLowLevelWrite(release, s);
		Pipe.publishWrites(release);
	}

	public static boolean handshakeProcessing(Pipe<NetPayloadSchema> pipe, BaseConnection con) {
		boolean result = true;
		if (null!=con) {			
			SSLEngine engine = con.getEngine();
			if (null != engine) {
				HandshakeStatus hanshakeStatus = engine.getHandshakeStatus();
				do {
					//logger.trace("handshake status {} ",hanshakeStatus);
				    if (HandshakeStatus.NEED_TASK == hanshakeStatus) {
				         Runnable task;
				         while ((task = engine.getDelegatedTask()) != null) {
				            	task.run(); 
				         }
				         hanshakeStatus = engine.getHandshakeStatus();
				    } 
				    
				    if (HandshakeStatus.NEED_WRAP == hanshakeStatus) {
				    	if (Pipe.hasRoomForWrite(pipe)) {
				    		//logger.trace("write handshake plain to trigger wrap");
				    		int size = Pipe.addMsgIdx(pipe, NetPayloadSchema.MSG_PLAIN_210);
				    		Pipe.addLongValue(con.getId(), pipe);//connection
				    		Pipe.addLongValue(System.currentTimeMillis(), pipe);
				    		Pipe.addLongValue(HANDSHAKE_POS, pipe); //signal that WRAP is needed 
				    		
				    		Pipe.addByteArray(OrderSupervisorStage.EMPTY, 0, 0, pipe);
				    		
				    		Pipe.confirmLowLevelWrite(pipe, size);
				    		Pipe.publishWrites(pipe);
				    		//wait for this to be consumed		    		
				    	} else {
				    		//no room to request wrap so try again later					
						}
				    	result = false;
				    	break;
				    } 
				} while ((HandshakeStatus.NEED_TASK == hanshakeStatus) || (HandshakeStatus.NEED_WRAP == hanshakeStatus));
			}
		}
		return result;
	}

}
