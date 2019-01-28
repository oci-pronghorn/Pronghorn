package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.ElapsedTimeRecorder;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

/**
 * Write to a socket using a client coordinator.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ClientSocketWriterStage extends PronghornStage {

	private static final Logger logger = LoggerFactory.getLogger(ClientSocketWriterStage.class);
		
	public static boolean logLatencyData = false;
		
	private final ClientCoordinator ccm;
	private final Pipe<NetPayloadSchema>[] input;
	
	private ByteBuffer[] buffers;
	private boolean[] bufferChecked;
	
	private ClientConnection[] connections;
	private ElapsedTimeRecorder[] latencyRecordings;
	
	private int shutCountDown;
	
	public static boolean showWrites = false;

	
	//FOR HEAVY LOAD TESTING THIS FEATURE MUST BE SWITCHED ON.
	private static final boolean enableWriteBatching = true;	
	
	//reqired for simulation of "slow" networks  TODO: read this from the client coordinator?
	private final boolean debugWithSlowWrites = false;
	private final int     debugMaxBlockSize = 50; 
	
	public static ClientSocketWriterStage newInstance(GraphManager graphManager, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] input) {
		return new ClientSocketWriterStage(graphManager, ccm, input);
	}

	/**
	 *
	 * @param graphManager
	 * @param ccm
	 * @param input _in_ Payload that will be written to socket.
	 */
	public ClientSocketWriterStage(GraphManager graphManager, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] input) {
		super(graphManager, input, NONE);
		if (input.length==0) {
			throw new UnsupportedOperationException("Unsupported configuration, stage must have at least 1 input.");
		}
		this.ccm = ccm;
		this.input = input;
		this.shutCountDown = input.length;

		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lavenderblush", this);
		if (input.length>1) {
			GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
		}
	}

	
	@Override
	public void startup() {
		
		int i = input.length;
		connections = new ClientConnection[i];
		latencyRecordings = new ElapsedTimeRecorder[i];
		buffers = new ByteBuffer[i];
		bufferChecked = new boolean[i];

	}
	
	@Override
	public void shutdown() {
		//long duration = System.currentTimeMillis()-start;
		//logger.trace("Client Bytes Written: {} kb/sec {} ",totalBytes, (8*totalBytes)/duration);

		if (logLatencyData) {
			ElapsedTimeRecorder etr = new ElapsedTimeRecorder();
			int i = latencyRecordings.length;
			int c = 0;
			while (--i>=0) {
				if (null!=latencyRecordings[i]) {
					c++;
					etr.add(latencyRecordings[i]);
				}
			}
			logger.info("latency data for {} connections \n{}",c,etr);
		}
		
	}
	
	@Override
	public void run() {
			
		boolean doingWork;
		boolean didWork = false;
			doingWork = false;	
			
			int i = input.length;
			while (--i >= 0) {

				if (connections[i]==null) {
					Pipe<NetPayloadSchema> pipe = input[i];
					while (connections[i]==null && Pipe.hasContentToRead(pipe)) {	
						doingWork |= writeAll(doingWork, i, pipe, Pipe.peekInt(pipe));						
					}
				} else {
					//we have multiple connections so one blocking does not impact others.
					doingWork |= tryWrite(i);					
				}
			}
			didWork |= doingWork;

		
//		//we have no pipes to monitor so this must be done explicitly
	    if (didWork && (null != this.didWorkMonitor)) {
	    	this.didWorkMonitor.published();
	    }

	}

	private boolean writeAll(boolean didWork, int i, Pipe<NetPayloadSchema> pipe, int msgIdx) {
		
		//For the close test we must be sending a plain then a disconnect NOT two plains!!
        //Any sequential Plain or Encrypted values will be rolled together at times on the same connection.
		
		if (NetPayloadSchema.MSG_PLAIN_210 == msgIdx) {							
			didWork = writePlain(didWork, i, pipe);
		} else {
			didWork = writeAllLessCommon(didWork, i, pipe, msgIdx);
		}
		return didWork;
	}

	private boolean writeAllLessCommon(boolean didWork, int i, Pipe<NetPayloadSchema> pipe, int msgIdx) {
		if (NetPayloadSchema.MSG_ENCRYPTED_200 == msgIdx) {											
			didWork = writeEncrypted(didWork, i, pipe);
		} else if (NetPayloadSchema.MSG_UPGRADE_307 == msgIdx) {							
			throw new UnsupportedOperationException("Connection upgrade is not yet supported.");
		} else if (NetPayloadSchema.MSG_BEGIN_208 == msgIdx) {							
			writeBegin(pipe);														
		} else if (NetPayloadSchema.MSG_DISCONNECT_203 == msgIdx) {							
			didWork = writeDisconnect(pipe);							
		} else {
			didWork = writeShutdown(didWork, pipe);		
		}
		return didWork;
	}

	private void writeBegin(Pipe<NetPayloadSchema> pipe) {
		int msgIdx = Pipe.takeMsgIdx(pipe);
		int seq = Pipe.takeInt(pipe);
		Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, NetPayloadSchema.MSG_BEGIN_208));
		Pipe.releaseReadLock(pipe);
	}

	private boolean writeDisconnect(Pipe<NetPayloadSchema> pipe) {
		long chnl = Pipe.peekLong(pipe, 0xF&NetPayloadSchema.MSG_DISCONNECT_203_FIELD_CONNECTIONID_201);
		ClientConnection cc = (ClientConnection)ccm.lookupConnectionById(chnl);
				
		int msgIdx = Pipe.takeMsgIdx(pipe);
		long channelId = Pipe.takeLong(pipe);
		assert(chnl==channelId);
		if (cc!=null) {
			if (cc.isValid()) {
				//only begin disconnect if not already disconnected
				cc.beginDisconnect();//do not close or we will not get any response
			} else {
				//already closed so remove value
				ccm.removeConnection(channelId);
			}
		} else {
			//already closed so remove value
			ccm.removeConnection(channelId);//already closed so remove if possible
		}
		
		Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
		Pipe.releaseReadLock(pipe);
		return true;
	}

	private boolean writeShutdown(boolean didWork, Pipe<NetPayloadSchema> pipe) {
		int msgIdx = Pipe.takeMsgIdx(pipe);
		if (msgIdx==-1) {
			Pipe.confirmLowLevelRead(pipe, Pipe.EOF_SIZE);								
		} else {
		    logger.info("unknown message idx received: {}",msgIdx);
		}
		Pipe.releaseReadLock(pipe);
		assert(-1 == msgIdx) : "Expected end of stream shutdown got "+msgIdx;
									
		if (--this.shutCountDown <= 0) {
			requestShutdown();
			didWork = false; //set to false so we exit.
		}
		return didWork;
	}

	private boolean writeEncrypted(boolean didWork, int i, Pipe<NetPayloadSchema> pipe) {
		long chnl = Pipe.peekLong(pipe, 0xF&NetPayloadSchema.MSG_ENCRYPTED_200_FIELD_CONNECTIONID_201);
		ClientConnection cc = (ClientConnection)ccm.lookupConnectionById(chnl);
			
		if (null==cc) {//closed or we can not get it yet, just push back till later.
			return false;
		}

		final int msgIdx = Pipe.takeMsgIdx(pipe);
		final long channelId = Pipe.takeLong(pipe);
		assert(chnl==channelId);
		final long arrivalTime = Pipe.takeLong(pipe);
		int meta = Pipe.takeByteArrayMetaData(pipe); //for string and byte array
		int len = Pipe.takeByteArrayLength(pipe);							
		
		if (showWrites) {
			logger.info("/////\n/// has connection "+((cc!=null)&&cc.isValid())+" channelId "+channelId+" write encrypted length:"+len);
			
		}
		
		didWork = wrapupUpEncryptedToSingleWrite(didWork, i, 
												pipe, msgIdx, channelId, meta, len,
												cc);
	
		return didWork;
	}

	int x = 0;
	private boolean writePlain(boolean didWork, int i, Pipe<NetPayloadSchema> pipe) {
		long chnl = Pipe.peekLong(pipe, 0xF&NetPayloadSchema.MSG_PLAIN_210_FIELD_CONNECTIONID_201);
		
		ClientConnection cc = (ClientConnection)ccm.lookupConnectionById(chnl);
		if (null==cc) {
			return false;
		}

		int msgIdx = Pipe.takeMsgIdx(pipe);
		long channelId = Pipe.takeLong(pipe);
		assert(channelId == chnl);
		long arrivalTime = Pipe.takeLong(pipe);
		
		long workingTailPosition = Pipe.takeLong(pipe);
					
		int meta = Pipe.takeByteArrayMetaData(pipe); //for string and byte array
		int len  = Pipe.takeByteArrayLength(pipe);

		if (showWrites) {
			int pos = Pipe.bytePosition(meta, pipe, len);
			logger.info("/////\n///pos "+pos+" has connection "+((cc!=null)&&cc.isValid())+" channelId "+channelId+
					"\n"+Appendables.appendUTF8(new StringBuilder(), Pipe.blob(pipe), pos, len, Pipe.blobMask(pipe)));
			
		}
		
		//no wrap is required so we have finished the TLS handshake and may continue
		if (SSLUtil.HANDSHAKE_POS != workingTailPosition) {	 						
				didWork = rollUpPlainsToSingleWrite(didWork, i, 
						pipe, msgIdx, channelId, cc, meta,
						len);
	
		} else {
			logger.error("Hanshake not supported here, this message should not have arrived");
			throw new UnsupportedOperationException("Check configuration, TLS handshake was not expected but requested. Check coordinator.");
		}
		return didWork;
	}

	private boolean wrapupUpEncryptedToSingleWrite(boolean didWork, int i, Pipe<NetPayloadSchema> pipe, int msgIdx,
			final long channelId, int meta, int len, ClientConnection cc) {
			
		ByteBuffer[] writeHolder = Pipe.wrappedReadingBuffers(pipe, meta, len);

		cc = checkBuffers(i, pipe, cc);
		
		assert(connections[i]==null);
		//copy done here to avoid GC and memory allocation done by socketChannel
		((Buffer)buffers[i]).clear();
		buffers[i].put(writeHolder[0]);
		buffers[i].put(writeHolder[1]);
		
		assert(writeHolder[0].remaining()==0);
		assert(writeHolder[1].remaining()==0);
										
		Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
		Pipe.releaseReadLock(pipe);
		

		if (null!=cc) {
			cc.recordSentTime(System.nanoTime());
		}
		
		while (enableWriteBatching && 
				Pipe.hasContentToRead(pipe) && 
		        Pipe.peekInt(pipe) == msgIdx && 
		        buffers[i].remaining() > pipe.maxVarLen && 
		        Pipe.peekLong(pipe, 1) == channelId ) {
		    			        	
		    	//logger.trace("opportunity found to batch writes going to {} ",channelId);
		    	
		    	int m = Pipe.takeMsgIdx(pipe);
		    	assert(m==msgIdx): "internal error";
		    	long c = Pipe.takeLong(pipe);
		    	long aTime = Pipe.takeLong(pipe);
		    	
		    	assert(c==channelId): "Internal error expected "+channelId+" but found "+c;

		        int meta2 = Pipe.takeByteArrayMetaData(pipe); //for string and byte array
		        int len2 = Pipe.takeByteArrayLength(pipe);
		        ByteBuffer[] writeBuffs2 = Pipe.wrappedReadingBuffers(pipe, meta2, len2);
		        
		        if (showWrites) {
					logger.info("/////\n/// has connection "+((cc!=null)&&cc.isValid())+" channelId "+channelId+" write encrypted length:"+len2);
					
				}
		        
		        buffers[i].put(writeBuffs2[0]);
		        buffers[i].put(writeBuffs2[1]);
		    									            
		        Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
		        Pipe.releaseReadLock(pipe);
		        
				if (null!=cc) {
					cc.recordSentTime(System.nanoTime());
				}
		    }	
//		System.err.println(enableWriteBatching+" && "+
//                Pipe.hasContentToRead(pipe)+" && "+
//                (Pipe.peekInt(pipe)==msgIdx)+" && "+ 
//		         (buffers[i].remaining()>pipe.maxVarLen)+" && "+ 
//		         (Pipe.peekLong(pipe, 1)==channelId) );
		
		((Buffer)buffers[i]).flip();	
		connections[i] = cc;
		latencyRecordings[i] = cc!=null?cc.histogram():null;//keep for later
		didWork |= tryWrite(i);
		return didWork;
	}

	//NOTE: if closed we can not "roll up" an can on use same cc instance!!
	
	private boolean rollUpPlainsToSingleWrite(boolean didWork, int i, Pipe<NetPayloadSchema> pipe, int msgIdx, long channelId,
			ClientConnection cc, int meta, int len) {

		ByteBuffer[] writeHolder = Pipe.wrappedReadingBuffers(pipe, meta, len);							

		checkBuffers(i, pipe, cc);
		
		assert(connections[i]==null);
		//copy done here to avoid GC and memory allocation done by socketChannel
		((Buffer)buffers[i]).clear();
		buffers[i].put(writeHolder[0]);
		buffers[i].put(writeHolder[1]);

		assert(writeHolder[0].remaining()==0);
		assert(writeHolder[1].remaining()==0);
										
		final int fragSize = Pipe.sizeOf(pipe, msgIdx);
		
		Pipe.confirmLowLevelRead(pipe, fragSize);
		Pipe.releaseReadLock(pipe);
		if (null!=cc) {
			cc.recordSentTime(System.nanoTime());
		}
//										System.err.println(enableWriteBatching+" && "+
//								                 Pipe.hasContentToRead(pipe)+" && "+
//							                     (Pipe.peekInt(pipe)==msgIdx)+" && "+ 
//					            		         (buffers[i].remaining()>pipe.maxVarLen)+" && "+ 
//					            		         (Pipe.peekLong(pipe, 1)==channelId) );										
		 while (enableWriteBatching 
				 && Pipe.hasContentToRead(pipe) 
				 && Pipe.peekInt(pipe)==msgIdx 
				 &&	buffers[i].remaining()>pipe.maxVarLen 
				 && Pipe.peekLong(pipe, 1)==channelId ) {
		        			        	
		        	//logger.trace("opportunity found to batch writes going to {} ",channelId);
		        	
		        	int m = Pipe.takeMsgIdx(pipe);
		        	assert(m==msgIdx): "internal error";
		        	long c = Pipe.takeLong(pipe);
		        	
		        	long aTime = Pipe.takeLong(pipe);
		        	
		        	assert(c==channelId): "Internal error expected "+channelId+" but found "+c;
		        	long workingTailPosition=Pipe.takeLong(pipe);
		        											            
		            int meta2 = Pipe.takeByteArrayMetaData(pipe); //for string and byte array
		            int len2 = Pipe.takeByteArrayLength(pipe);
		            
		            if (showWrites) {
		            	int pos2 = Pipe.bytePosition(meta2, pipe, len2);							
						Appendables.appendUTF8(System.out, Pipe.blob(pipe), pos2, len2, Pipe.blobMask(pipe));
		            }
		            
		            ByteBuffer[] writeBuffs2 = Pipe.wrappedReadingBuffers(pipe, meta2, len2);
		            
		            buffers[i].put(writeBuffs2[0]);
		            buffers[i].put(writeBuffs2[1]);
		        		
			        Pipe.confirmLowLevelRead(pipe, fragSize);
			        Pipe.releaseReadLock(pipe);
					if (null!=cc) {
						cc.recordSentTime(System.nanoTime());
					}
		}											
		
//		 System.err.println(enableWriteBatching+" && "+
//	                Pipe.hasContentToRead(pipe)+" && "+
//	                (Pipe.peekInt(pipe)==msgIdx)+" && "+ 
//			         (buffers[i].remaining()>pipe.maxVarLen)+" && "+ 
//			         (Pipe.peekLong(pipe, 1)==channelId) );
		 

		((Buffer)buffers[i]).flip();	
		connections[i] = cc;
		latencyRecordings[i] = null!=cc?cc.histogram():null;//keep for later
		
		didWork |= tryWrite(i);
		return didWork;
	}

	private ClientConnection checkBuffers(int i, Pipe<NetPayloadSchema> pipe, ClientConnection cc) {
		if (!bufferChecked[i]) {
			if (null!=cc) {
				SocketChannel sc = cc.getSocketChannel();
				if (null!=sc) {
					try {
						int minBufSize = Math.max(pipe.sizeOfBlobRing/4, //buffer should be as large as the full pipe so more can accumulate
								                  sc.getOption(StandardSocketOptions.SO_SNDBUF));
						if (null!=buffers[i]) {
							logger.trace("buffer is {} and must be at least {}",buffers[i].capacity(), minBufSize);
						}
						if (null==buffers[i] || buffers[i].capacity()<minBufSize) {
							//logger.info("new direct buffer of size {} created old one was too small.",minBufSize);
							buffers[i] = ByteBuffer.allocateDirect(minBufSize);
						}
						bufferChecked[i] = true;
					} catch (IOException e) {
						return null;
					}
				}
			}
		}
		return cc;
	}

	
	
	private boolean tryWrite(int i) {
		ClientConnection cc = connections[i];
		ByteBuffer mappedByteBuffer = buffers[i];
		if (cc!=null && cc.isValid() && !cc.isDisconnecting()) {
			SocketChannel sc = cc.getSocketChannel();
			if (sc!=null) {			
				//NOT Required: assert(mappedByteBuffer.hasRemaining()) : "please, do not call if there is nothing to write."; 	
				try {
					
					if (!debugWithSlowWrites) {
						assert(mappedByteBuffer.isDirect());
						
						//required to push large loads.
						while (mappedByteBuffer.hasRemaining()) {
							if (connections[i].getSocketChannel().write(mappedByteBuffer)<=0) {
								break;//can't write now, try later.
							};
							Thread.yield();//this is also important for large loads
						}
					} else {
						//write only this many bytes over the network at a time
						ByteBuffer buf = ByteBuffer.wrap(new byte[debugMaxBlockSize]);
						buf.clear();
						
						int j = debugMaxBlockSize;
						int c = mappedByteBuffer.remaining();
						int p = mappedByteBuffer.position();
						while (--c>=0 && --j>=0) {
							buf.put(mappedByteBuffer.get(p++));
						}
						mappedByteBuffer.position(p);
						
						buf.flip();
						int expected = buf.limit();
						
						while (buf.hasRemaining()) {
							int len = connections[i].getSocketChannel().write(buf);
							if (len>0) {
								expected-=len;
							}
						}
						if (expected!=0) {
							throw new UnsupportedOperationException();
						}
						
						//logger.info("remaining to write {} for {}",buffers[i].remaining(),i);
						
					}
				} catch (IOException e) {
					
					// if e.message is  "Broken pipe" then the connection was already lost, nothing to do here but close.
					//logger.debug("Client side connection closing, excption while writing to socket for Id {}.",connections[i].getId() ,e);
					
					connections[i].close();
					connections[i]=null;
					((Buffer)mappedByteBuffer).clear();
					return true;
				}
				if (!mappedByteBuffer.hasRemaining()) {
				
					//logger.info("write clear {}",i);
					((Buffer)mappedByteBuffer).clear();
					connections[i]=null;
					return true;
				}  else {
					
		//			if (Integer.numberOfLeadingZeros(countOfUnableToFullyWrite) != 
		//				Integer.numberOfLeadingZeros(countOfUnableToFullyWrite++)) {							
		//				logger.info("Network overload issues on connection {} we still have {} bytes wating to write ",
		//						i,buffers[i].remaining());				
		//			}
					return false;
				}
			}
		}
		//this connection is closed so we have noplace to write the data, dump it
		((Buffer)mappedByteBuffer).clear();
		connections[i]=null;
		return true;
	}

	

}
