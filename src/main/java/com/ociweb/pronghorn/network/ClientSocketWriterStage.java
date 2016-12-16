package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ClientSocketWriterStage extends PronghornStage {
	
	private static final Logger logger = LoggerFactory.getLogger(ClientSocketWriterStage.class);
	
	private final ClientCoordinator ccm;
	private final Pipe<NetPayloadSchema>[] input;
	private ByteBuffer[] buffers;
	private ClientConnection[] connections;
	
	private int shutCountDown;

	private long start;
	private long totalBytes=0;
	
	
	public static ClientSocketWriterStage newInstance(GraphManager graphManager, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] input) {
		return new ClientSocketWriterStage(graphManager, ccm, input);
	}
	
	public ClientSocketWriterStage(GraphManager graphManager, ClientCoordinator ccm, Pipe<NetPayloadSchema>[] input) {
		super(graphManager, input, NONE);
		this.ccm = ccm;
		this.input = input;
		this.shutCountDown = input.length;
	}

	
	@Override
	public void startup() {
		int BUF_SIZE = 4;
		int i = input.length;
		connections = new ClientConnection[i];
		buffers = new ByteBuffer[i];
		while (--i>=0) {
			buffers[i] = ByteBuffer.allocateDirect(input[i].maxAvgVarLen*BUF_SIZE);
		}
		start = System.currentTimeMillis();		
	}
	
	@Override
	public void shutdown() {
		long duration = System.currentTimeMillis()-start;
		
		logger.info("Client Bytes Written: {} kb/sec {} ",totalBytes, (8*totalBytes)/duration);
		
	}
	
	@Override
	public void run() {
		
		boolean didWork;
		
		do {
			didWork = false;
						
			int i = input.length;
			while (--i>=0) {
								
				if (connections[i]!=null) {
	//PHI hit this on network TODO: urgent.				System.err.println("first write did not finish");
					tryWrite(i);
					if (connections[i]==null) {
						didWork = true;
//						i++; //run me again to check for new content
//						continue;
					}
				} else {
					Pipe<NetPayloadSchema> pipe = input[i];
					
					int msgIdx = -1;
					//if here helps balance out the traffic so no single users gets backed up.
					if (connections[i]==null && Pipe.hasContentToRead(pipe)) try {			
	
						msgIdx = Pipe.takeMsgIdx(pipe);
						
						//System.err.println("data to read "+Pipe.contentRemaining(input[i])+"    "+PipeReader.getMsgIdx(input[i]));
														
						if (NetPayloadSchema.MSG_ENCRYPTED_200 == msgIdx) {
											
							final long channelId = Pipe.takeLong(pipe);
							int meta = Pipe.takeRingByteMetaData(pipe); //for string and byte array
							int len = Pipe.takeRingByteLen(pipe);

							ClientConnection cc = (ClientConnection)ccm.get(channelId, 0);
	
							if (null!=cc) {
						        
								int payloadSize = len;
								totalBytes += payloadSize;
								
								ByteBuffer[] writeHolder = Pipe.wrappedReadingBuffers(pipe, meta, len);
								
	
								//copy done here to avoid GC and memory allocation done by socketChannel
								buffers[i].clear();
								buffers[i].put(writeHolder[0]);
								buffers[i].put(writeHolder[1]);
								
//								boolean enableWriteBatching = true;
//								boolean takeTail = false;
								
								Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
								Pipe.releaseReadLock(pipe);
								
//								System.err.println(enableWriteBatching+" && "+
//								                 Pipe.hasContentToRead(pipe)+" && "+
//							                     (Pipe.peekInt(pipe)==msgIdx)+" && "+ 
//					            		         (buffers[i].remaining()>pipe.maxAvgVarLen)+" && "+ 
//					            		         (Pipe.peekLong(pipe, 1)==channelId) );
//								
//						        while (enableWriteBatching && Pipe.hasContentToRead(pipe) && 
//							            Pipe.peekInt(pipe)==msgIdx && 
//							            		buffers[i].remaining()>pipe.maxAvgVarLen && 
//							            Pipe.peekLong(pipe, 1)==channelId ) {
//							        			        	
//							        	//logger.trace("opportunity found to batch writes going to {} ",channelId);
//							        	
//							        	int m = Pipe.takeMsgIdx(pipe);
//							        	assert(m==msgIdx): "internal error";
//							        	long c = Pipe.takeLong(pipe);
//							        	assert(c==channelId): "Internal error expected "+channelId+" but found "+c;
//							        	
//							        	
////							            if (takeTail) {
////							            	activeTails[idx] =  Pipe.takeLong(pipe);
////							            } else {
////							            	activeTails[idx] = -1;
////							            }
//							            
//							            int meta2 = Pipe.takeRingByteMetaData(pipe); //for string and byte array
//							            int len2 = Pipe.takeRingByteLen(pipe);
//							            ByteBuffer[] writeBuffs2 = Pipe.wrappedReadingBuffers(pipe, meta2, len2);
//							            
//							            buffers[i].put(writeBuffs2[0]);
//							            buffers[i].put(writeBuffs2[1]);
//							        		
//							            System.err.println("did it here");
//							            
//								        Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
//								        Pipe.releaseReadLock(pipe);
//							        }	
								
								buffers[i].flip();	
								connections[i] = cc;
								
								tryWrite(i);

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
							ClientConnection cc = (ClientConnection)ccm.get(channelId, 0);
							
							long workingTail = Pipe.takeLong(pipe);
							
							int meta = Pipe.takeRingByteMetaData(pipe); //for string and byte array
							int len = Pipe.takeRingByteLen(pipe);
	
						    	if (SSLUtil.HANDSHAKE_POS != workingTail) {
		 							
									if (null!=cc) {

										totalBytes += len;

										ByteBuffer[] writeHolder = Pipe.wrappedReadingBuffers(pipe, meta, len);
							
										//copy done here to avoid GC and memory allocation done by socketChannel
										buffers[i].clear();
										buffers[i].put(writeHolder[0]);
										buffers[i].put(writeHolder[1]);
										
										Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
										Pipe.releaseReadLock(pipe);
										
//										boolean enableWriteBatching = true;

//										System.err.println(enableWriteBatching+" && "+
//								                 Pipe.hasContentToRead(pipe)+" && "+
//							                     (Pipe.peekInt(pipe)==msgIdx)+" && "+ 
//					            		         (buffers[i].remaining()>pipe.maxAvgVarLen)+" && "+ 
//					            		         (Pipe.peekLong(pipe, 1)==channelId) );
										
//										 while (enableWriteBatching && Pipe.hasContentToRead(pipe) && 
//										            Pipe.peekInt(pipe)==msgIdx && 
//										            		buffers[i].remaining()>pipe.maxAvgVarLen && 
//										            Pipe.peekLong(pipe, 1)==channelId ) {
//										        			        	
//										        	//logger.trace("opportunity found to batch writes going to {} ",channelId);
//										        	
//										        	int m = Pipe.takeMsgIdx(pipe);
//										        	assert(m==msgIdx): "internal error";
//										        	long c = Pipe.takeLong(pipe);
//										        	assert(c==channelId): "Internal error expected "+channelId+" but found "+c;
//										        	
//										        	boolean takeTail = true;
//										        	if (takeTail) {
//										        		workingTail=Pipe.takeLong(pipe);
//										        	}
//										        	
////										            if (takeTail) {
////										            	activeTails[idx] =  Pipe.takeLong(pipe);
////										            } else {
////										            	activeTails[idx] = -1;
////										            }
//										            
//										            int meta2 = Pipe.takeRingByteMetaData(pipe); //for string and byte array
//										            int len2 = Pipe.takeRingByteLen(pipe);
//										            ByteBuffer[] writeBuffs2 = Pipe.wrappedReadingBuffers(pipe, meta2, len2);
//										            
//										            buffers[i].put(writeBuffs2[0]);
//										            buffers[i].put(writeBuffs2[1]);
//										        		
//										      //      System.err.println("did it here");
//										            
//											        Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
//											        Pipe.releaseReadLock(pipe);
//										        }											
//										
										
										
										
										//System.err.println("write size of "+buffers[i].position());
										
										buffers[i].flip();	
													
										
										
										connections[i] = cc;
										
//										try {
//											cc.socketChannel.socket().setTcpNoDelay(true);
//											//buffer size:1313280
//											System.out.println("buffer size:"+cc.socketChannel.socket().getSendBufferSize());
//											//cc.socketChannel.socket().sendUrgentData(data); ?? any usages?
//													
//										} catch (SocketException e) {
//											// TODO Auto-generated catch block
//											e.printStackTrace();
//										}
										
										
										tryWrite(i);
									} else {
										logger.error("GGGGGGGGGGGGGGGggggg client lost connection and did not send datafor {} ", channelId);
										//TODO: this in important case to test.
										//can not send this connection was lost
										Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
										Pipe.releaseReadLock(pipe);
										continue;
									}
						    	} else {
						    		logger.error("Hanshake not supported here, this message should not have arrived");
						    	}
						} else {
							
							Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, msgIdx));
							Pipe.releaseReadLock(pipe);
							
							assert(-1 == msgIdx) : "Expected end of stream shutdown";
							
							if (--this.shutCountDown <= 0) {
								requestShutdown();
								return;
							}		
						}
								
						
					} finally {
						didWork = true;
						
					}			
				}
				
			}
		} while (didWork);
		
	}

	private void tryWrite(int i) {
		assert(buffers[i].hasRemaining()) : "please, do not call if there is nothing to write.";		
		try {
			connections[i].getSocketChannel().write(buffers[i]);
		} catch (IOException e) {
			logger.info("excption while writing to socket. ",e);
			connections[i].close();
		}
		if (!buffers[i].hasRemaining()) {
			connections[i]=null;
		}
	}

}
