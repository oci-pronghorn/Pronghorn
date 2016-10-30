package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class SSLUtil {

	final static ByteBuffer noData = ByteBuffer.allocate(0);
	final static ByteBuffer[] noDatas = new ByteBuffer[]{noData,noData};
	
	
	public static boolean handShakeWrapIfNeeded(SSLConnection cc, Pipe<NetPayloadSchema> target, ByteBuffer buffer) {
		 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();
		 
		 if (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus &&
				 HandshakeStatus.FINISHED != handshakeStatus	 ) {
			 			 
			 if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {
				 return true;//done by the other stage.
			 }
			 
			 if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
				 return SSLUtil.handshakeWrapLogic(cc, target, buffer);
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
	
	public static boolean handshakeWrapLogic(SSLConnection cc, Pipe<NetPayloadSchema> target, ByteBuffer buffer) {
	    
		try {
			final ByteBuffer[] targetBuffers = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
			SSLEngineResult result = cc.getEngine().wrap(noDatas, targetBuffers[0]);
			Status status = SSLUtil.wrapResultStatusState(target, buffer, cc, noDatas, targetBuffers, result);
			
			if (Status.OK == status) {
				
				PipeWriter.writeLong(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201, cc.getId());
				
				PipeWriter.publishWrites(target);
				HandshakeStatus handshakeStatus = result.getHandshakeStatus();
				
				return (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus) && (HandshakeStatus.FINISHED != handshakeStatus);
				
			}
		} catch (SSLException e) {
			PipeWriter.wrappedUnstructuredLayoutBufferCancel(target);
		}	
		
	    return false;	
	    
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
				SSLEngineResult result2 = cc.getEngine().wrap(bbHolder, buffer);
				
				status = result2.getStatus();
				if (status == Status.OK) {
					
					//write buffer to openA and openB
					buffer.flip();
					
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
					if (buffer.hasRemaining()) {
						throw new UnsupportedOperationException("ERR1");
					}
					PipeWriter.tryWriteFragment(target, NetPayloadSchema.MSG_ENCRYPTED_200);
					PipeWriter.wrappedUnstructuredLayoutBufferClose(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203, result2.bytesProduced());
					
				} else {
					throw new UnsupportedOperationException("ERR2");
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

	public static SSLEngineResult unwrappedResultStatusState(Pipe<NetPayloadSchema> source, ByteBuffer rolling,
			SSLConnection cc, final ByteBuffer[] targetBuffer) {
		
		SSLEngineResult result=null;
				
		ByteBuffer[] inputs = PipeReader.wrappedUnstructuredLayoutBuffer(source, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
		
		cc.localRunningBytesProduced = 0;
		if (inputs[1].remaining()==0) {
			//if we have some rolling data from previously
			if (rolling.position()==0) {
				try {
					result = cc.getEngine().unwrap(inputs[0], targetBuffer);
					cc.localRunningBytesProduced += result.bytesProduced();
				
					rolling.put(inputs[0]);//keep anything left for next time.
					assert(0==inputs[0].remaining());
					
					while (result.getStatus() == Status.OK && rolling.position()>0) {					
						rolling.flip();
						result = cc.getEngine().unwrap(rolling, targetBuffer);
						cc.localRunningBytesProduced += result.bytesProduced();
						
		
						rolling.compact();
					}
				} catch (SSLException sslex) {
					manageException(sslex, cc);
					System.err.println("no unwrap, exception");
					result = null;
				}
				
				
			} else {
				//add this new content onto the end before use.
				rolling.put(inputs[0]);
				assert(0==inputs[0].remaining());
			
				//flip so unwrap can see it
				rolling.flip();
				try {
					//Takes single buffer in. NOTE: NONE OF THIS LOGIC WOULD HAVE BEEN REQURIED IF SSLEngine.unwrap took ByteBuffer[] instead of a single !!
					result = cc.getEngine().unwrap(rolling, targetBuffer);
					cc.localRunningBytesProduced += result.bytesProduced();
				
					rolling.compact();	
					
					while (result.getStatus() == Status.OK && rolling.position()>0) {
						rolling.flip();
					    result = cc.getEngine().unwrap(rolling, targetBuffer);
						cc.localRunningBytesProduced += result.bytesProduced();
													
						rolling.compact();
					}
				} catch (SSLException sslex) {
					manageException(sslex, cc);
					System.err.println("no unwrap, exception 3");
					result = null;
					
				}
				
			}
		} else {
			//working this is confirmed as used.
			//System.out.println("***************  A and B ");
			assert(inputs[0].hasRemaining());
			assert(inputs[1].hasRemaining());
			
			rolling.put(inputs[0]);  //slow copy
			rolling.put(inputs[1]);  //slow copy
			rolling.flip();
			try {
				result = cc.getEngine().unwrap(rolling, targetBuffer);
				cc.localRunningBytesProduced += result.bytesProduced();
				
				rolling.compact();
				
				while (result.getStatus() == Status.OK && rolling.position()>0) {
					rolling.flip();
					result = cc.getEngine().unwrap(rolling, targetBuffer);
					cc.localRunningBytesProduced += result.bytesProduced();
					
					rolling.compact();
				}
				
			} catch (SSLException sslex) {
				manageException(sslex, cc);
				System.err.println("no unwrap, exception 5");
				result = null;
				
			}
		}
	
		PipeReader.releaseReadLock(source);	
		
		return result;
	}

	public static boolean handShakeUnWrapIfNeeded(SSLConnection cc, Pipe<NetPayloadSchema> source, ByteBuffer rolling, ByteBuffer[] workspace, 
			                                      Pipe<NetPayloadSchema> handshakePipe, ByteBuffer secureBuffer) {
		
		 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();
		 
		 if (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus &&
			 HandshakeStatus.FINISHED != handshakeStatus	 ) {
		
			 if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
				 if (null == handshakePipe) {
					 //Client Mode
					 return true;//done by the other stage. 
				 } else {
					 //Server Mode
					 return handshakeWrapLogic(cc, handshakePipe, secureBuffer);
				 }
			 }
			 
			 if (HandshakeStatus.NEED_UNWRAP == handshakeStatus) {
								 
				 	if (!PipeReader.tryReadFragment(source)) {
				 		throw new UnsupportedOperationException();
				 	}
				 	
				 	int msgId = PipeReader.getMsgIdx(source);
				 					 	
				    //if closed closd
	                if (msgId<0 || !cc.isValid) {	                	
	                	PipeReader.releaseReadLock(source);
	                    if (cc.getEngine().isInboundDone() && cc.getEngine().isOutboundDone()) {
	                        return true;
	                    }	                    
	                    handshakeStatus = cc.closeInboundCloseOutbound(cc.getEngine());
	                    return true;
	                }
	    			
	    			SSLEngineResult result = unwrappedResultStatusState(source, rolling, cc, workspace);
	    			
	    			handshakeStatus = result.getHandshakeStatus();
					Status status = null!=result?result.getStatus():null;
	
					if (Status.CLOSED == status) {
						 if (cc.getEngine().isOutboundDone()) {
							 	cc.close();
		                    	return true;	
		                    } else {
		                        cc.getEngine().closeOutbound();
		                        handshakeStatus = cc.getEngine().getHandshakeStatus();
		                    }
					}
				 
				    return (HandshakeStatus.NOT_HANDSHAKING != handshakeStatus) && (HandshakeStatus.FINISHED != handshakeStatus);
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

	/**
	 * Encrypt as much as possible based on the data available from the two pipes
	 * @param source
	 * @param target
	 * @param buffer 
	 */
	public static void engineWrap(SSLConnectionHolder ccm, Pipe<NetPayloadSchema> source, Pipe<NetPayloadSchema> target, ByteBuffer buffer, boolean isServer) {
		
		while (PipeWriter.hasRoomForWrite(target) && PipeReader.peekMsg(source, NetPayloadSchema.MSG_PLAIN_210) ) {
			
			final SSLConnection cc = ccm.get(PipeReader.peekLong(source, NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201), 0);
			
			if (null==cc || !cc.isValid) {
				//do not process this message because the connection has dropped
				PipeReader.tryReadFragment(source);
				PipeReader.releaseReadLock(source);
				continue;
			}
			
			if ((!isServer) && handShakeWrapIfNeeded(cc, target, buffer)) {
				return;
			}
			
			PipeReader.tryReadFragment(source);
			
			ByteBuffer[] bbHolder = PipeReader.wrappedUnstructuredLayoutBuffer(source, NetPayloadSchema.MSG_PLAIN_210_FIELD_PAYLOAD_204);										
	
			try {
				final ByteBuffer[] targetBuffers = PipeWriter.wrappedUnstructuredLayoutBufferOpen(target, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_PAYLOAD_203);
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
			                        ByteBuffer rolling, ByteBuffer[] workspace, Pipe<NetPayloadSchema> handshakePipe, ByteBuffer secureBuffer) {
		
		while (PipeReader.hasContentToRead(source) ) {
			
			if (!PipeWriter.hasRoomForWrite(target)) {								
				return;//try again later when there is room in the output
			}			
				
			
			SSLConnection cc = ccm.get(PipeReader.peekLong(source, NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201), 0);
			
			if (null==cc || !cc.isValid) {
				//do not process this message because the connection has dropped
				PipeReader.tryReadFragment(source);
				PipeReader.releaseReadLock(source);
				continue;
			}
			
			if (handShakeUnWrapIfNeeded(cc, source, rolling, workspace, handshakePipe, secureBuffer)) {
				return;
			}
			
			if (!PipeReader.tryReadFragment(source)) {
				return;//not an error because the only fragment may have been consumed by the handshake
			}
					
			if (PipeReader.getMsgIdx(source)<0) {
				PipeWriter.publishEOF(target);
				PipeReader.releaseReadLock(source);
				//requestShutdown();
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
			
			
			SSLEngineResult result = unwrappedResultStatusState(source, rolling, cc, writeHolderUnWrap);
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
