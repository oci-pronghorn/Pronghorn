package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ServiceObjectHolder;

public class ServerSocketWriterStage extends PronghornStage {
    
    private static Logger logger = LoggerFactory.getLogger(ServerSocketWriterStage.class);
   
    private final Pipe<NetPayloadSchema>[] dataToSend;
    private final Pipe<ReleaseSchema> releasePipe;
    
    private final ServerCoordinator coordinator;


    public final static int UPGRADE_TARGET_PIPE_MASK     = (1<<21)-1;
 
    public final static int UPGRADE_CONNECTION_SHIFT     = 31;    
    public final static int UPGRADE_MASK                 = 1<<UPGRADE_CONNECTION_SHIFT;
    
    public final static int CLOSE_CONNECTION_SHIFT       = 30;
    public final static int CLOSE_CONNECTION_MASK        = 1<<CLOSE_CONNECTION_SHIFT;
    
    public final static int END_RESPONSE_SHIFT           = 29;//for multi message send this high bit marks the end
    public final static int END_RESPONSE_MASK            = 1<<END_RESPONSE_SHIFT;
    
    public final static int INCOMPLETE_RESPONSE_SHIFT    = 28;
    public final static int INCOMPLETE_RESPONSE_MASK     = 1<<INCOMPLETE_RESPONSE_SHIFT;
    
    
    private ByteBuffer workingBuffers[];
    private SocketChannel writeToChannel[];
    private long          writeToChannelId[];
    private int           writeToChannelMsg[];
    private int           writeToChannelTTL[]; //TODO: this must be time based not count based.
    			    		
    
    private long activeTails[];
    private long activeIds[]; 
    private int activeMessageIds[];    
  
    private long totalBytesWritten = 0;
    
    //TODO: add param for small to use just 4 ?? //TODO: this must be changed to max latency wait not fixed cycles, important.
    private int bufferMultiplier = 16;//12; //NOTE: larger buffer allows for faster xmit.


	private static final boolean enableWriteBatching = true;  
    
    private StringBuilder[] accumulators;

	private final boolean debugWithSlowWrites = false; //TODO: set from coordinator, NOTE: this is a critical piece of the tests
	private final int debugMaxBlockSize = 7;//50000;
	
    
    /**
     * 
     * Writes pay-load back to the appropriate channel based on the channelId in the message.
     * 
     * + ServerResponseSchema is custom to this stage and supports all the features here
     * + Has support for upgrade redirect pipe change (Module can clear bit to prevent this if needed)
     * + Has support for closing connection after write as needed for HTTP 1.1 and 0.0
     * 
     * 
     * + Will Have support for writing same pay-load to multiple channels (subscriptions)
     * + Will Have support for order enforcement and pipelined requests
     * 
     * 
     * @param graphManager
     * @param coordinator
     * @param dataToSend
     */
    public ServerSocketWriterStage(GraphManager graphManager, ServerCoordinator coordinator, Pipe<NetPayloadSchema>[] dataToSend) {
        super(graphManager, dataToSend, NONE);
        this.coordinator = coordinator;
        this.dataToSend = dataToSend;
        this.releasePipe = null;
    }
    
    //optional ack mode for testing and other configuraitons..  
    
    public ServerSocketWriterStage(GraphManager graphManager, ServerCoordinator coordinator, Pipe<NetPayloadSchema>[] input, Pipe<ReleaseSchema> releasePipe) {
        super(graphManager, input, releasePipe);
        this.coordinator = coordinator;
        this.dataToSend = input;
        this.releasePipe = releasePipe;
    }
    
    @Override
    public void startup() {
    	
		if (ServerCoordinator.TEST_RECORDS) {
			int i = dataToSend.length;
			accumulators = new StringBuilder[i];
			while (--i >= 0) {
				accumulators[i]=new StringBuilder();					
			}
		}
    	
    	
    	
    	int c = dataToSend.length;
    	writeToChannel = new SocketChannel[c];
    	writeToChannelId = new long[c];
    	writeToChannelMsg = new int[c];
    	writeToChannelTTL = new int[c];
    	
    	workingBuffers = new ByteBuffer[c];
    	activeTails = new long[c];
    	activeIds = new long[c];
    	activeMessageIds = new int[c];
    	int capacity = bufferMultiplier*maxVarLength(dataToSend);
    	//System.err.println("allocating "+capacity+" for "+c);
    	
    	while (--c>=0) {    	
			workingBuffers[c] = ByteBuffer.allocateDirect(capacity);    		
    	}
    	Arrays.fill(activeTails, -1);
    	
    }
    
    @Override
    public void shutdown() {

    }
    
    @Override
    public void run() {
       
    	boolean didWork = false;
    	do {
    		didWork = false;
	    	int x = dataToSend.length;
	    	while (--x>=0) {
	    		//logger.info("server socket writer run {}",x);
	    		
	    		if (null == writeToChannel[x]) {
	    			if (Pipe.hasContentToRead(dataToSend[x])) {
	    			//	logger.info("data to read for {}",x);
	    				didWork = true;
		            	int activeMessageId = Pipe.takeMsgIdx(dataToSend[x]);
		            			            	
		            	processMessage(activeMessageId, x);
		            	
		            	if (activeMessageId < 0) {
		            		continue;
		            	}
	    			}// else {
	    			//	logger.info("no data to read {} {} ",x,dataToSend[x]);
	    			//}
	    		} else {
	    			//logger.info("write the channel");
	    			    			
	    			boolean hasRoomToWrite = workingBuffers[x].capacity()-workingBuffers[x].limit() > dataToSend[x].maxAvgVarLen;
	    				        			
	    			
	    			if (--writeToChannelTTL[x]<=0 || !hasRoomToWrite) {
	    				writeToChannelMsg[x] = -1;
		    			didWork = true;
		    			writeToChannel(x); 
		   		    			
	    			} else {
	    				
	    		
	    				//unflip
	    				int p = workingBuffers[x].limit();
	    				workingBuffers[x].limit(workingBuffers[x].capacity());
	    				workingBuffers[x].position(p);	    				
	    				
	    				int h = 0;
	    				while (	isNextMessageMergeable(dataToSend[x], writeToChannelMsg[x], x, writeToChannelId[x], false) ) {
	    					h++;
	    					//logger.info("opportunity found to batch writes going to {} ", writeToChannelId[x]);
	    						    					
	    					mergeNextMessage(writeToChannelMsg[x], x, dataToSend[x], writeToChannelId[x]);
	    						    					
	    				}	
	    				if (h>0) {
	    					Pipe.releaseAllPendingReadLock(dataToSend[x]);
	    				}
	    				workingBuffers[x].flip();
	    				
//	    				if ( Pipe.hasContentToRead(dataToSend[x]) || h==0) { //TODO: or if end disoverd?	    				
//		    				writeToChannelMsg[x] = -1;		    				
//			    			didWork = true;
//			    			writeToChannel(x); 
//	    				}
	    				
	    			}
	    			
	    		}    		
	    		
	    	}
	    	
    	} while (didWork);
        
    	boolean debug = false;
    	if (debug) {					
			if (lastTotalBytes!=totalBytesWritten) {
				System.err.println("Server writer total bytes :"+totalBytesWritten);
				lastTotalBytes =totalBytesWritten;
			}
    	}
    	
    }
    
    long lastTotalBytes = 0;

	private void processMessage(int activeMessageId, int idx) {
		
		activeMessageIds[idx] = activeMessageId;
		
		//logger.info("sever to write {}",activeMessageId);
				
		if ( (NetPayloadSchema.MSG_PLAIN_210 == activeMessageId) ||
		     (NetPayloadSchema.MSG_ENCRYPTED_200 == activeMessageId) ) {
			            		
			loadPayloadForXmit(activeMessageId, idx);
//			if (null!=writeToChannel[idx]) {
//				writeToChannel(idx);
//			}
		
		} else if (NetPayloadSchema.MSG_DISCONNECT_203 == activeMessageId) {
					
			final long channelId = Pipe.takeLong(dataToSend[idx]);
			//logger.info("DISCONNECT MESSAGE FOUND BY SOCKET WRITER {} ",channelId);
			
		    Pipe.confirmLowLevelRead(dataToSend[idx], Pipe.sizeOf(dataToSend[idx], activeMessageId));
		    Pipe.releaseReadLock(dataToSend[idx]);
		    assert(Pipe.contentRemaining(dataToSend[idx])>=0);
		    
		    coordinator.releaseResponsePipeLineIdx(channelId);//upon disconnect let go of pipe reservation
		    ServiceObjectHolder<ServerConnection> socketHolder = ServerCoordinator.getSocketChannelHolder(coordinator);
		    if (null!=socketHolder) {
		        ServerConnection serverConnection = socketHolder.get(channelId);	          
		        if (null!=serverConnection) {
		        	closeChannel(serverConnection.getSocketChannel()); 
		        }
		    }	                    
		    
		} else if (NetPayloadSchema.MSG_UPGRADE_307 == activeMessageId) {
			
			final long channelId = Pipe.takeLong(dataToSend[idx]);
			final int newRoute = Pipe.takeInt(dataToSend[idx]);
			
		    //set the pipe for any further communications
		   // ServerCoordinator.setTargetUpgradePipeIdx(coordinator, groupIdx, channelId, newRoute);
		    
		    Pipe.confirmLowLevelRead(dataToSend[idx], Pipe.sizeOf(dataToSend[idx], activeMessageId));
		    Pipe.releaseReadLock(dataToSend[idx]);
		    assert(Pipe.contentRemaining(dataToSend[idx])>=0);
		    
		} else if (NetPayloadSchema.MSG_BEGIN_208 == activeMessageId) {
			
			int seqNo = Pipe.takeInt(dataToSend[idx]);
			Pipe.confirmLowLevelRead(dataToSend[idx], Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_BEGIN_208));
			Pipe.releaseReadLock(dataToSend[idx]);
			
		} else if (activeMessageId < 0) {
		    
			Pipe.confirmLowLevelRead(dataToSend[idx], Pipe.EOF_SIZE);
		    Pipe.releaseReadLock(dataToSend[idx]);
		    assert(Pipe.contentRemaining(dataToSend[idx])>=0);
		    
		    //comes from muliple pipes so this can not be done yet.
		    //requestShutdown();	                    
		}
	}

	
    private void loadPayloadForXmit(final int msgIdx, final int idx) {
        
    	final boolean takeTail = NetPayloadSchema.MSG_PLAIN_210 == msgIdx;
    	final int msgSize = Pipe.sizeOf(dataToSend[idx], msgIdx);
    	
        Pipe<NetPayloadSchema> pipe = dataToSend[idx];
        final long channelId = Pipe.takeLong(pipe);
        final long arrivalTime = Pipe.takeLong(pipe);        
        
        activeIds[idx] = channelId;
        if (takeTail) {
        	activeTails[idx] =  Pipe.takeLong(pipe);
        } else {
        	activeTails[idx] = -1;
        }
        //byteVector is payload
        int meta = Pipe.takeRingByteMetaData(pipe); //for string and byte array
        int len = Pipe.takeRingByteLen(pipe);
        
        
        //System.err.println(this.stageId+"writer Ch:"+channelId+" len:"+len+" from pipe "+idx);
                
        ServiceObjectHolder<ServerConnection> socketHolder = ServerCoordinator.getSocketChannelHolder(coordinator);
        
        if (null!=socketHolder) {
	        ServerConnection serverConnection = socketHolder.get(channelId);
	        	        
	        //only write if this connection is still valid
	        if (null != serverConnection) {        
				
	        	writeToChannel[idx] = serverConnection.getSocketChannel(); //ChannelId or SubscriptionId      
	        	writeToChannelId[idx] = channelId;
	        	writeToChannelMsg[idx] = msgIdx;
	        	writeToChannelTTL[idx] = 16;
	        	
	        	
		        //logger.debug("write {} to socket for id {}",len,channelId);
		        
		        ByteBuffer[] writeBuffs = Pipe.wrappedReadingBuffers(pipe, meta, len);
		        
		        workingBuffers[idx].clear();
		        workingBuffers[idx].put(writeBuffs[0]);
		        workingBuffers[idx].put(writeBuffs[1]);
		        
		        assert(!writeBuffs[0].hasRemaining());
		        assert(!writeBuffs[1].hasRemaining());
		        		       		        
		        Pipe.confirmLowLevelRead(dataToSend[idx], msgSize);
		        
		        Pipe.readNextWithoutReleasingReadLock(dataToSend[idx]);
		        //Pipe.releaseReadLock(dataToSend[idx]);
		        
		        //In order to maximize throughput take all the messages which are gong to the same location.

		        //if there is content and this content is also a message to send and we still have room in the working buffer and the channel is the same then we can batch it.
		        while (enableWriteBatching && isNextMessageMergeable(pipe, msgIdx, idx, channelId, false) ) {		        			        	
		        	//logger.trace("opportunity found to batch writes going to {} ",channelId);
		        	
		        	mergeNextMessage(msgIdx, idx, pipe, channelId);
			        			      
		        }	
		        
		        Pipe.releaseAllPendingReadLock(dataToSend[idx]);
		        
		        
		        if (ServerCoordinator.TEST_RECORDS) {
		        	ByteBuffer temp = workingBuffers[idx].duplicate();
		        	temp.flip();
		        	testValidContent(idx, temp);
		        }
		        
		      //  logger.info("total bytes written {} ",totalBytesWritten);
		        
		       // logger.info("write bytes {} for id {}",workingBuffers[idx].position(),channelId);
		        
		        workingBuffers[idx].flip();
	        } else {
	        	//logger.debug("no server connection found for id:{}",channelId);
		        
		        Pipe.confirmLowLevelRead(pipe, msgSize);
		        Pipe.releaseReadLock(pipe);
	        }

        } else {
        	logger.error("Can not write, too early because SocketChannelHolder has not yet been created");
        }
                
    }

	private void mergeNextMessage(final int msgIdx, final int idx, Pipe<NetPayloadSchema> pipe, final long channelId) {
		
		final boolean takeTail = NetPayloadSchema.MSG_PLAIN_210 == msgIdx;
		
		int m = Pipe.takeMsgIdx(pipe);
		assert(m==msgIdx): "internal error";
		long c = Pipe.takeLong(pipe);
		
		long aTime = Pipe.takeLong(pipe);
		assert(c==channelId): "Internal error expected "+channelId+" but found "+c;
		
		
		if (takeTail) {
			activeTails[idx] =  Pipe.takeLong(pipe);
		} else {
			activeTails[idx] = -1;
		}
		int meta2 = Pipe.takeRingByteMetaData(pipe); //for string and byte array
		int len2 = Pipe.takeRingByteLen(pipe);
		ByteBuffer[] writeBuffs2 = Pipe.wrappedReadingBuffers(pipe, meta2, len2);
		
		workingBuffers[idx].put(writeBuffs2[0]);
		workingBuffers[idx].put(writeBuffs2[1]);
		
		assert(!writeBuffs2[0].hasRemaining());
		assert(!writeBuffs2[1].hasRemaining());
				        		
		Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(NetPayloadSchema.instance, msgIdx));
		Pipe.readNextWithoutReleasingReadLock(dataToSend[idx]);
	}

	private boolean isNextMessageMergeable(Pipe<NetPayloadSchema> pipe, final int msgIdx, final int idx, final long channelId, boolean debug) {

		if (debug) {
		    logger.info("Data {} {} {} {} ",
		    		    Pipe.hasContentToRead(pipe),
		    		    Pipe.peekInt(pipe)==msgIdx,
		    		    workingBuffers[idx].remaining()>pipe.maxAvgVarLen,
		    		    Pipe.peekLong(pipe, 1)==channelId	    		
		    		);
		}
		
		return  Pipe.hasContentToRead(pipe) && 
				Pipe.peekInt(pipe)==msgIdx && 
				workingBuffers[idx].remaining()>pipe.maxAvgVarLen && 
				Pipe.peekLong(pipe, 1)==channelId;
	}

    int totalB;
	private void testValidContent(final int idx, ByteBuffer buf) {
	
		if (ServerCoordinator.TEST_RECORDS) {
							
			
			boolean confirmExpectedRequests = true;
			if (confirmExpectedRequests) {
			
				 
				int pos = buf.position();
				int len = buf.remaining();
				
				
				while (--len>=0) {
					totalB++;
					accumulators[idx].append((char)buf.get(pos++));
				}
				
			//	Appendables.appendUTF8(accumulators[idx], buf.array(), pos, len, Integer.MAX_VALUE);						    				
				
				while (accumulators[idx].length() >= ServerCoordinator.expectedOK.length()) {
					
				   int c = startsWith(accumulators[idx],ServerCoordinator.expectedOK); 
				   if (c>0) {
					   
					   String remaining = accumulators[idx].substring(c*ServerCoordinator.expectedOK.length());
					   accumulators[idx].setLength(0);
					   accumulators[idx].append(remaining);							    					   
					   
					   
				   } else {
					   logger.info("A"+Arrays.toString(ServerCoordinator.expectedOK.getBytes()));
					   logger.info("B"+Arrays.toString(accumulators[idx].subSequence(0, ServerCoordinator.expectedOK.length()).toString().getBytes()   ));
					   
					   logger.info("FORCE EXIT ERROR exlen {} BAD BYTE BUFFER at {}",ServerCoordinator.expectedOK.length(),totalB);
					   System.out.println(accumulators[idx].subSequence(0, ServerCoordinator.expectedOK.length()).toString());
					   System.exit(-1);
					   	
					   
					   
				   }
				
					
				}
			}
			
			
		}
	}
    
	private void testValidContent(final int idx, Pipe<NetPayloadSchema> pipe, int meta, int len) {
	
		if (ServerCoordinator.TEST_RECORDS) {
			
			//write pipeIdx identifier.
			//Appendables.appendUTF8(System.out, target.blobRing, originalBlobPosition, readCount, target.blobMask);
			
			int pos = Pipe.convertToPosition(meta, pipe);
			    				
			
			boolean confirmExpectedRequests = true;
			if (confirmExpectedRequests) {
				Appendables.appendUTF8(accumulators[idx], pipe.blobRing, pos, len, pipe.blobMask);						    				
				
				while (accumulators[idx].length() >= ServerCoordinator.expectedOK.length()) {
					
				   int c = startsWith(accumulators[idx],ServerCoordinator.expectedOK); 
				   if (c>0) {
					   
					   String remaining = accumulators[idx].substring(c*ServerCoordinator.expectedOK.length());
					   accumulators[idx].setLength(0);
					   accumulators[idx].append(remaining);							    					   
					   
					   
				   } else {
					   logger.info("A"+Arrays.toString(ServerCoordinator.expectedOK.getBytes()));
					   logger.info("B"+Arrays.toString(accumulators[idx].subSequence(0, ServerCoordinator.expectedOK.length()).toString().getBytes()   ));
					   
					   logger.info("FORCE EXIT ERROR at {} exlen {}",pos,ServerCoordinator.expectedOK.length());
					   System.out.println(accumulators[idx].subSequence(0, ServerCoordinator.expectedOK.length()).toString());
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

    private void writeToChannel(int idx) {

    		
    		if (!debugWithSlowWrites) {
		        try {
		        	//assert(workingBuffers[idx] instanceof sun.nio.ch.DirectBuffer) : "should be direct??";
		        	int bytesWritten = writeToChannel[idx].write(workingBuffers[idx]);	  
		        	
		        	//short blocks of bytes written may be slowdown!!
		        	//System.err.println("wrote bytes:" + bytesWritten);
		        	
		        	//TODO: urgent hold some writes until we have more, but do write immidiate if we have seen a different connection id.
		        	
		        	if (bytesWritten>0) {
		        		totalBytesWritten+=bytesWritten;
		        	}
		        	if (!workingBuffers[idx].hasRemaining()) {
		        		markDoneAndRelease(idx);
		        	} 
		        
		        } catch (IOException e) {
		        	//logger.trace("unable to write to channel",e);
		        	closeChannel(writeToChannel[idx]);
		            //unable to write to this socket, treat as closed
		            markDoneAndRelease(idx);
		        }
    		} else {
				
				ByteBuffer buf = ByteBuffer.wrap(new byte[debugMaxBlockSize]);
				buf.clear();
				
				int j = debugMaxBlockSize;
				int c = workingBuffers[idx].remaining();

				int p = workingBuffers[idx].position();
				while (--c>=0 && --j>=0) {
					buf.put(workingBuffers[idx].get(p++));
				}
				workingBuffers[idx].position(p);
								
				
				buf.flip();
				int expected = buf.limit();
				
				while (buf.hasRemaining()) {
					try {
						int len = writeToChannel[idx].write(buf);
						if (len>0) {
							expected -= len;
							totalBytesWritten += len;
						}
					} catch (IOException e) {
						//logger.error("unable to write to channel {} '{}'",e,e.getLocalizedMessage());
						closeChannel(writeToChannel[idx]);
			            //unable to write to this socket, treat as closed
			            markDoneAndRelease(idx);
			            
			            return;
					}
				}
				if (expected!=0) {
					throw new UnsupportedOperationException();
				}
							
	        	if (!workingBuffers[idx].hasRemaining()) {
	        		markDoneAndRelease(idx);
	        	}
    			
    		}	        
	        
	        

    }

    private void closeChannel(SocketChannel channel) {
        try {
        	if (channel.isOpen()) {
        		channel.close();
        	}
        } catch (IOException e1) {
            logger.warn("unable co close channel",e1);
        }
    }

    private void markDoneAndRelease(int idx) {
    	//logger.trace("write is complete for {} ", activeIds[idx]);
       
    	workingBuffers[idx].clear();
    	writeToChannel[idx]=null;
        int sequenceNo = 0;//not available here
        if (null!=releasePipe) {
        	if (!Pipe.hasRoomForWrite(releasePipe)) {
        		logger.info("warning must block until pipe is free or we may create a hang condition.");
        		Pipe.spinBlockForRoom(releasePipe, Pipe.sizeOf(releasePipe, ReleaseSchema.MSG_RELEASEWITHSEQ_101));
        	}        	
        	if (Pipe.hasRoomForWrite(releasePipe)) {
        		publishRelease(releasePipe, activeIds[idx],
        				        activeTails[idx]!=-1?activeTails[idx]: Pipe.tailPosition(dataToSend[idx]),
        				        sequenceNo);
        	} else {
        		logger.info("potential hang from failure to release pipe");
        	}
        }
    }

    

	private static void publishRelease(Pipe<ReleaseSchema> pipe, long conId, long position, int sequenceNo) {
		assert(position!=-1);
		//logger.debug("sending release for {} at position {}",conId,position);
		
		int size = Pipe.addMsgIdx(pipe, ReleaseSchema.MSG_RELEASEWITHSEQ_101);
		Pipe.addLongValue(conId, pipe);
		Pipe.addLongValue(position, pipe);
		Pipe.addIntValue(sequenceNo, pipe);
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
				
	}
}
