package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

public class ClientSocketWriterStage extends PronghornStage {
	
	private static final Logger logger = LoggerFactory.getLogger(ClientSocketWriterStage.class);
	
	private final ClientCoordinator ccm;
	private final Pipe<NetPayloadSchema>[] input;
	private ByteBuffer[] buffers;
	private ClientConnection[] connections;
	
	private int shutCountDown;

	private long start;
	private long totalBytes=0;
	
	//FOR HEAVY LOAD TESTING THIS FEATURE MUST BE SWITCHED ON.
	private static final boolean enableWriteBatching = true;		
	
	//reqired for simulation of "slow" networks  TODO: read this from the client coordinator?
	private final boolean debugWithSlowWrites = false;
	private final int     debugMaxBlockSize = 50; 
	
	//NOTE: this is important for high volume testing is must be large enough to push out data at a fast rate.
	private final int bufMultiplier;
	
	public static ClientSocketWriterStage newInstance(GraphManager graphManager, ClientCoordinator ccm, int bufMultiplier, Pipe<NetPayloadSchema>[] input) {
		return new ClientSocketWriterStage(graphManager, ccm, bufMultiplier, input);
	}
	
	public ClientSocketWriterStage(GraphManager graphManager, ClientCoordinator ccm, int bufMultiplier, Pipe<NetPayloadSchema>[] input) {
		super(graphManager, input, NONE);
		if (input.length==0) {
			throw new UnsupportedOperationException("Unsupported configuration, stage must have at least 1 input.");
		}
		this.ccm = ccm;
		this.input = input;
		this.shutCountDown = input.length;
		this.bufMultiplier = bufMultiplier;
				
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lavenderblush", this);
	}

	
	@Override
	public void startup() {
		
		int i = input.length;
		connections = new ClientConnection[i];
		buffers = new ByteBuffer[i];
		start = System.currentTimeMillis();		
	}
	
	@Override
	public void shutdown() {
		//long duration = System.currentTimeMillis()-start;
		//logger.trace("Client Bytes Written: {} kb/sec {} ",totalBytes, (8*totalBytes)/duration);
		
	}
	
	@Override
	public void run() {
		
		boolean didWork;
		
		do {
			didWork = false;
		
			int i = input.length;
			while (--i>=0) {
								
				if (connections[i]!=null) {
					//we have multiple connections so one blocking does not impact others.
					didWork |= tryWrite(i);
				} 

				if (connections[i]==null) {
					Pipe<NetPayloadSchema> pipe = input[i];
					assert(pipe.bytesReadBase(pipe)>=0);
					
					int msgIdx = -1;
					//if here helps balance out the traffic so no single users gets backed up.
					if (connections[i]==null && Pipe.hasContentToRead(pipe)) {			
	
						msgIdx = Pipe.takeMsgIdx(pipe);
						
						if (NetPayloadSchema.MSG_ENCRYPTED_200 == msgIdx) {
											
							final long channelId = Pipe.takeLong(pipe);
							final long arrivalTime = Pipe.takeLong(pipe);
							int meta = Pipe.takeRingByteMetaData(pipe); //for string and byte array
							int len = Pipe.takeRingByteLen(pipe);							
							

							ClientConnection cc = (ClientConnection)ccm.connectionForSessionId(channelId);
	
							if (null!=cc) {
						        
								didWork = wraupUpEncryptedToSingleWrite(didWork, i, pipe, msgIdx, channelId, meta, len,
										cc);

							} else {
								//clean shutdown of this connections resources
								buffers[i].clear();
								connections[i]=null;
								Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
								Pipe.releaseReadLock(pipe);
								continue;
							}
						} else if (NetPayloadSchema.MSG_PLAIN_210 == msgIdx) {
							
							long channelId = Pipe.takeLong(pipe);
							long arrivalTime = Pipe.takeLong(pipe);
							ClientConnection cc = (ClientConnection)ccm.connectionForSessionId(channelId);
							
							long workingTailPosition = Pipe.takeLong(pipe);
										
							int meta = Pipe.takeRingByteMetaData(pipe); //for string and byte array
							int len  = Pipe.takeRingByteLen(pipe);
	
							final boolean showWrittenData = false;
							if (showWrittenData) {
								int pos = Pipe.bytePosition(meta, pipe, len);	
								System.out.println("pos "+pos+" has connection "+(cc!=null)+" channelId "+channelId);
								Appendables.appendUTF8(System.out, Pipe.blob(pipe), pos, len, Pipe.blobMask(pipe));
							}
							
								//no wrap is required so we have finished the TLS handshake and may continue
						    	if (SSLUtil.HANDSHAKE_POS != workingTailPosition) {
		 						
									if (null!=cc) {
										
										didWork = rollUpPlainsToSingleWrite(didWork, i, 
												pipe, msgIdx, channelId, cc, meta,
												len, showWrittenData);
									} else {
									
										//can not send this connection was lost, consume and drop the data to get it off the pipe
										Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
										Pipe.releaseReadLock(pipe);
										continue;
									}
						    	} else {
						    		logger.error("Hanshake not supported here, this message should not have arrived");
						    		throw new UnsupportedOperationException("Check configuration, TLS handshake was not expected but requested. Check coordinator.");
						    	}
						} else if (NetPayloadSchema.MSG_UPGRADE_307 == msgIdx) {
							
							throw new UnsupportedOperationException("Connection upgrade is not yet supported.");
						
						} else if (NetPayloadSchema.MSG_BEGIN_208 == msgIdx) {
							
							int seq = Pipe.takeInt(pipe);
							Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, NetPayloadSchema.MSG_BEGIN_208));
							Pipe.releaseReadLock(pipe);
							
							
						} else if (NetPayloadSchema.MSG_DISCONNECT_203 == msgIdx) {
							
							long channelId = Pipe.takeLong(pipe);
							ClientConnection cc = (ClientConnection)ccm.connectionForSessionId(channelId);
							if (null!=cc) {
								cc.close();
							}
							
							Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
							Pipe.releaseReadLock(pipe);
							
						} else {
							if (msgIdx==-1) {
								Pipe.confirmLowLevelRead(pipe, Pipe.EOF_SIZE);								
							} else {
							    logger.info("unknown message idx received: {}",msgIdx);
							}
							Pipe.releaseReadLock(pipe);
							assert(-1 == msgIdx) : "Expected end of stream shutdown got "+msgIdx;
														
							if (--this.shutCountDown <= 0) {
								requestShutdown();
								return;
							}		
						}
								
						
					} 
					assert(pipe.bytesReadBase(pipe)>=0);
				}
				
			}
		} while (didWork);
		
	}

	private boolean wraupUpEncryptedToSingleWrite(boolean didWork, int i, Pipe<NetPayloadSchema> pipe, int msgIdx,
			final long channelId, int meta, int len, ClientConnection cc) {
		int payloadSize = len;
		totalBytes += payloadSize;
		
		ByteBuffer[] writeHolder = Pipe.wrappedReadingBuffers(pipe, meta, len);
		
		if (null==buffers[i]) {
			try {
				int sendBufSize = 
						Math.max(pipe.maxVarLen, 
						         cc.socketChannel.getOption(StandardSocketOptions.SO_SNDBUF));
				logger.info("new direct buffer of size {}",sendBufSize);
				buffers[i] = ByteBuffer.allocateDirect(sendBufSize);						
			} catch (IOException e) {
				new RuntimeException(e);
			}
		}
		
		assert(connections[i]==null);
		//copy done here to avoid GC and memory allocation done by socketChannel
		buffers[i].clear();
		buffers[i].put(writeHolder[0]);
		buffers[i].put(writeHolder[1]);
		
		assert(writeHolder[0].remaining()==0);
		assert(writeHolder[1].remaining()==0);
										
		Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
		Pipe.releaseReadLock(pipe);
		
//								System.err.println(enableWriteBatching+" && "+
//								                 Pipe.hasContentToRead(pipe)+" && "+
//							                     (Pipe.peekInt(pipe)==msgIdx)+" && "+ 
//					            		         (buffers[i].remaining()>pipe.maxAvgVarLen)+" && "+ 
//					            		         (Pipe.peekLong(pipe, 1)==channelId) );
		
		cc.recordSentTime(System.nanoTime());
		
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

		        int meta2 = Pipe.takeRingByteMetaData(pipe); //for string and byte array
		        int len2 = Pipe.takeRingByteLen(pipe);
		        ByteBuffer[] writeBuffs2 = Pipe.wrappedReadingBuffers(pipe, meta2, len2);
		        
		        buffers[i].put(writeBuffs2[0]);
		        buffers[i].put(writeBuffs2[1]);
		    									            
		        Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
		        Pipe.releaseReadLock(pipe);
		        
		        cc.recordSentTime(System.nanoTime());
		    }	
		
		((Buffer)buffers[i]).flip();	
		connections[i] = cc;

		didWork |= tryWrite(i);
		return didWork;
	}

	private boolean rollUpPlainsToSingleWrite(boolean didWork, int i, Pipe<NetPayloadSchema> pipe, int msgIdx, long channelId,
			ClientConnection cc, int meta, int len, boolean showWrittenData) {
		long workingTailPosition;
		totalBytes += len;
		ByteBuffer[] writeHolder = Pipe.wrappedReadingBuffers(pipe, meta, len);							

		if (null==buffers[i]) {
			try {
				int sendBufSize = 
						Math.max(pipe.maxVarLen, 
						         cc.socketChannel.getOption(StandardSocketOptions.SO_SNDBUF));
				logger.info("new direct buffer of size {}",sendBufSize);
				buffers[i] = ByteBuffer.allocateDirect(sendBufSize);						
			} catch (IOException e) {
				new RuntimeException(e);
			}
		}
		
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
		
		cc.recordSentTime(System.nanoTime());
		
//										System.err.println(enableWriteBatching+" && "+
//								                 Pipe.hasContentToRead(pipe)+" && "+
//							                     (Pipe.peekInt(pipe)==msgIdx)+" && "+ 
//					            		         (buffers[i].remaining()>pipe.maxAvgVarLen)+" && "+ 
//					            		         (Pipe.peekLong(pipe, 1)==channelId) );										
		 while (enableWriteBatching && Pipe.hasContentToRead(pipe) && 
		            Pipe.peekInt(pipe)==msgIdx && 
		            		buffers[i].remaining()>pipe.maxVarLen && 
		            Pipe.peekLong(pipe, 1)==channelId ) {
		        			        	
		        	logger.trace("opportunity found to batch writes going to {} ",channelId);
		        	
		        	int m = Pipe.takeMsgIdx(pipe);
		        	assert(m==msgIdx): "internal error";
		        	long c = Pipe.takeLong(pipe);
		        	
		        	long aTime = Pipe.takeLong(pipe);
		        	
		        	assert(c==channelId): "Internal error expected "+channelId+" but found "+c;
		        	workingTailPosition=Pipe.takeLong(pipe);
		        											            
		            int meta2 = Pipe.takeRingByteMetaData(pipe); //for string and byte array
		            int len2 = Pipe.takeRingByteLen(pipe);
		            
		            if (showWrittenData) {
		            	int pos2 = Pipe.bytePosition(meta2, pipe, len2);							
						Appendables.appendUTF8(System.out, Pipe.blob(pipe), pos2, len2, Pipe.blobMask(pipe));
		            }
		            
		            ByteBuffer[] writeBuffs2 = Pipe.wrappedReadingBuffers(pipe, meta2, len2);
		            
		            buffers[i].put(writeBuffs2[0]);
		            buffers[i].put(writeBuffs2[1]);
		        		
			        Pipe.confirmLowLevelRead(pipe, fragSize);
			        Pipe.releaseReadLock(pipe);
			        
			        cc.recordSentTime(System.nanoTime());
			      
		}											
		

		((Buffer)buffers[i]).flip();	
		connections[i] = cc;
		
		didWork |= tryWrite(i);
		return didWork;
	}

	
	
	private boolean tryWrite(int i) {
		ByteBuffer mappedByteBuffer = buffers[i];
		assert(mappedByteBuffer.hasRemaining()) : "please, do not call if there is nothing to write.";	
		
		try {
			
			if (!debugWithSlowWrites) {
				assert(mappedByteBuffer.isDirect());	
				while (connections[i].getSocketChannel().write(mappedByteBuffer)>0) {
					//keep writing the output buffer may be small
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
						
			this.ccm.releaseResponsePipeLineIdx(connections[i].getId());
			connections[i].close();
			connections[i]=null;
			mappedByteBuffer.clear();
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
	private int countOfUnableToFullyWrite = 0;
	
 //   private int totalB;
    
//	private int startsWith(StringBuilder stringBuilder, String expected2) {
//		
//		int count = 0;
//		int rem = stringBuilder.length();
//		int base = 0;
//		while(rem>=expected2.length()) {
//			int i = expected2.length();
//			while (--i>=0) {
//				if (stringBuilder.charAt(base+i)!=expected2.charAt(i)) {
//					return count;
//				}
//			}
//			base+=expected2.length();
//			rem-=expected2.length();
//			count++;
//		}
//		return count;
//	}
	

}
