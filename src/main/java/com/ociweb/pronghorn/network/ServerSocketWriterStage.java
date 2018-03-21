package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.http.HTTPUtil;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ServiceObjectHolder;

public class ServerSocketWriterStage extends PronghornStage {
    
    private static Logger logger = LoggerFactory.getLogger(ServerSocketWriterStage.class);
    private final static boolean showAllContentSent = false;
    
    private final Pipe<NetPayloadSchema>[] input;
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
    
    
    private MappedByteBuffer workingBuffers[];
    private SocketChannel writeToChannel[];
    private long          writeToChannelId[];
    private int           writeToChannelMsg[];
    private int           writeToChannelBatchCountDown[]; 
    
    private long activeTails[];
    private long activeIds[]; 
    private int activeMessageIds[];    
  
    private long totalBytesWritten = 0;
   
    private final int bufferMultiplier;
    private int maxBatchCount;

	private static final boolean enableWriteBatching = true;  
    
    private StringBuilder[] accumulators;

	private final boolean debugWithSlowWrites = false; //TODO: set from coordinator, NOTE: this is a critical piece of the tests
	private final int debugMaxBlockSize = 7;//50000;
	
	private GraphManager graphManager;
	
    
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
    public ServerSocketWriterStage(GraphManager graphManager, ServerCoordinator coordinator, int bufferMultiplier, Pipe<NetPayloadSchema>[] dataToSend) {
        super(graphManager, dataToSend, NONE);
        this.coordinator = coordinator;
        this.input = dataToSend;
        this.releasePipe = null;
        this.bufferMultiplier = bufferMultiplier;
        this.graphManager = graphManager;
        
        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
        GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
    }
    
    
    //optional ack mode for testing and other configuraitons..  
    
    public ServerSocketWriterStage(GraphManager graphManager, ServerCoordinator coordinator, int bufferMultiplier, Pipe<NetPayloadSchema>[] input, Pipe<ReleaseSchema> releasePipe) {
        super(graphManager, input, releasePipe);
        this.coordinator = coordinator;
        this.input = input;
        this.releasePipe = releasePipe;
        this.bufferMultiplier = bufferMultiplier;
        
        
        
        this.graphManager = graphManager;
       
        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
        GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
    }
    
    @Override
    public void startup() {
    	
    	final Number rate = (Number)GraphManager.getNota(graphManager, this, GraphManager.SCHEDULE_RATE, null);
    	    	
    	//this is 20ms for the default max limit
    	long hardLimtNS = 20_000_000; //May revisit later.
        //also note however data can be written earlier if:
    	//   1. the buffer has run out of space (the multiplier controls this)
    	//   2. if the pipe has no more data.
    	
    	this.maxBatchCount = null==rate ? 16 : (int)(hardLimtNS/rate.longValue());
    	    	
		if (ServerCoordinator.TEST_RECORDS) {
			int i = input.length;
			accumulators = new StringBuilder[i];
			while (--i >= 0) {
				accumulators[i]=new StringBuilder();					
			}
		} 	
    	
    	
    	int c = input.length;
    	if (c > (1<<12)) {
    		System.err.println("warning, server socket writer allocated n long arrays of length"+c);
    	}
    	writeToChannel = new SocketChannel[c];
    	writeToChannelId = new long[c];
    	writeToChannelMsg = new int[c];
    	writeToChannelBatchCountDown = new int[c];
    	
    	workingBuffers = new MappedByteBuffer[c];
    	activeTails = new long[c];
    	activeIds = new long[c];
    	activeMessageIds = new int[c];
    	int capacity = bufferMultiplier*maxVarLength(input);
    	
    	//System.err.println("allocating "+capacity+" for "+c);
    	
    	while (--c>=0) {    	
			workingBuffers[c] = (MappedByteBuffer)ByteBuffer.allocateDirect(capacity);    		
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
	    	int x = input.length;
	    	while (--x>=0) {
	    		Pipe<NetPayloadSchema> localInput = input[x];
	    		
	    		if (null == writeToChannel[x]) {
	    			
	    			if (Pipe.hasContentToRead(localInput)) {
	    				didWork = true;
		            	int activeMessageId = Pipe.takeMsgIdx(localInput);		            			            	
		            	processMessage(activeMessageId, x);
		            	if (activeMessageId < 0) {
		            		continue;
		            	}	
	    			} else {	    				
	    				//no content to read on the pipe
	    				//all the old data has been written so the writeChannel is null	    		
	    			}
	    			
	    		} else {
	    			//logger.info("write the channel");
	    			ByteBuffer localWorkingBuffer = workingBuffers[x];
	    			
	    			boolean hasRoomToWrite = localWorkingBuffer.capacity()-localWorkingBuffer.limit() > localInput.maxVarLen;
	    			//note writeToChannelBatchCountDown is set to zero when nothing else can be combined...
	    			if (--writeToChannelBatchCountDown[x]<=0 
	    				|| !hasRoomToWrite
	    				|| !Pipe.hasContentToRead(localInput) //myPipeHasNoData so fire now
	    				) {
	    				writeToChannelMsg[x] = -1;
		    			didWork = writeDataToChannel(x); 
		   		    			
	    			} else {
	    				
	    				//unflip
	    				int p = localWorkingBuffer.limit();
	    				localWorkingBuffer.limit(localWorkingBuffer.capacity());
	    				localWorkingBuffer.position(p);	    				
	    				
	    				int h = 0;
	    				while (	isNextMessageMergeable(localInput, writeToChannelMsg[x], x, writeToChannelId[x], false) ) {
	    					h++;
	    					//logger.info("opportunity found to batch writes going to {} ", writeToChannelId[x]);
	    						    					
	    					mergeNextMessage(writeToChannelMsg[x], x, localInput, writeToChannelId[x]);
	    						    					
	    				}	
	    				if (Pipe.hasContentToRead(localInput)) {
	    					writeToChannelBatchCountDown[x] = 0;//send now nothing else is mergable
	    				}
	    				
	    				if (h>0) {
	    					Pipe.releaseAllPendingReadLock(localInput);
	    				}
	    				localWorkingBuffer.flip();

	    			}
	    			
	    		}    		
	    		
	    	}
	    	
    	} while (didWork);

    }
    
    long lastTotalBytes = 0;

	private void processMessage(int activeMessageId, int idx) {
		
		activeMessageIds[idx] = activeMessageId;
		
		//logger.info("sever to write {}",activeMessageId);
				
		if ( (NetPayloadSchema.MSG_PLAIN_210 == activeMessageId) ||
		     (NetPayloadSchema.MSG_ENCRYPTED_200 == activeMessageId) ) {
			            		
			loadPayloadForXmit(activeMessageId, idx);

		} else if (NetPayloadSchema.MSG_DISCONNECT_203 == activeMessageId) {
					
			final long channelId = Pipe.takeLong(input[idx]);

		    Pipe.confirmLowLevelRead(input[idx], Pipe.sizeOf(input[idx], activeMessageId));
		    Pipe.releaseReadLock(input[idx]);
		    assert(Pipe.contentRemaining(input[idx])>=0);
		    
		    coordinator.releaseResponsePipeLineIdx(channelId);//upon disconnect let go of pipe reservation
		    ServiceObjectHolder<ServerConnection> socketHolder = ServerCoordinator.getSocketChannelHolder(coordinator);
		    if (null!=socketHolder) {
		    	//we are disconnecting so we will remove the connection from the holder.
		        ServerConnection serverConnection = socketHolder.remove(channelId);	          
		        if (null!=serverConnection) {
		        	serverConnection.close();
		        }
		    }	                    
		    
		   // logger.info("finished the disconnect");
		    
		} else if (NetPayloadSchema.MSG_UPGRADE_307 == activeMessageId) {
			
			//set the pipe for any further communications
		    long channelId = Pipe.takeLong(input[idx]);
			int pipeIdx = Pipe.takeInt(input[idx]);
			
			
			ServerCoordinator.setUpgradePipe(coordinator, 
		    		channelId, //connection Id 
		    		pipeIdx); //pipe idx
		    
		    //switch to new reserved connection?? after upgrade no need to use http router
		    //perhaps? coordinator.releaseResponsePipeLineIdx(channelId);
		    //	 connection   setPoolReservation
		    //or...
			
		    
		    Pipe.confirmLowLevelRead(input[idx], Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_UPGRADE_307));
		    Pipe.releaseReadLock(input[idx]);
		    assert(Pipe.contentRemaining(input[idx])>=0);
		    
		} else if (NetPayloadSchema.MSG_BEGIN_208 == activeMessageId) {

			int seqNo = Pipe.takeInt(input[idx]);
			Pipe.confirmLowLevelRead(input[idx], Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_BEGIN_208));
			Pipe.releaseReadLock(input[idx]);
			
		} else if (activeMessageId < 0) {
		    
			Pipe.confirmLowLevelRead(input[idx], Pipe.EOF_SIZE);
		    Pipe.releaseReadLock(input[idx]);
		    assert(Pipe.contentRemaining(input[idx])>=0);
		    
		    //comes from muliple pipes so this can not be done yet.
		    //requestShutdown();	                    
		}
	}

	
    private void loadPayloadForXmit(final int msgIdx, final int idx) {
        
    	final boolean takeTail = NetPayloadSchema.MSG_PLAIN_210 == msgIdx;
    	final int msgSize = Pipe.sizeOf(input[idx], msgIdx);
    	
        Pipe<NetPayloadSchema> pipe = input[idx];
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
        
        if (showAllContentSent) {
        	System.out.println("////////////////////");
        	int pos = Pipe.convertToPosition(meta, pipe);
        	Appendables.appendUTF8(System.out, Pipe.blob(pipe), pos, len, Pipe.blobMask(pipe));
        	System.out.println("////////////////////");
        }
        
        
        //System.err.println(this.stageId+"writer Ch:"+channelId+" len:"+len+" from pipe "+idx);
                
        ServiceObjectHolder<ServerConnection> socketHolder = ServerCoordinator.getSocketChannelHolder(coordinator);
        
        if (null!=socketHolder) {
	        ServerConnection serverConnection = socketHolder.get(channelId);
	        	        
	        //only write if this connection is still valid
	        if (null != serverConnection) {        
					    
	        	//System.err.println("new conection attached");
	        	
	        	writeToChannel[idx] = serverConnection.getSocketChannel(); //ChannelId or SubscriptionId      
	        	writeToChannelId[idx] = channelId;
	        	writeToChannelMsg[idx] = msgIdx;
	        	writeToChannelBatchCountDown[idx] = maxBatchCount;
	        	
	        	
		        //logger.debug("write {} to socket for id {}",len,channelId);
		        
	        	
	        	
		        ByteBuffer[] writeBuffs = Pipe.wrappedReadingBuffers(pipe, meta, len);
		        
		        workingBuffers[idx].clear();
		        workingBuffers[idx].put(writeBuffs[0]);
		        workingBuffers[idx].put(writeBuffs[1]);
		        
		        assert(!writeBuffs[0].hasRemaining());
		        assert(!writeBuffs[1].hasRemaining());
		        		       		        
		        Pipe.confirmLowLevelRead(input[idx], msgSize);
		        
		        Pipe.readNextWithoutReleasingReadLock(input[idx]);
		        //Pipe.releaseReadLock(dataToSend[idx]);
		        
		        //In order to maximize throughput take all the messages which are gong to the same location.

		        //if there is content and this content is also a message to send and we still have room in the working buffer and the channel is the same then we can batch it.
		        while (enableWriteBatching && isNextMessageMergeable(pipe, msgIdx, idx, channelId, false) ) {		        			        	
		        	//logger.trace("opportunity found to batch writes going to {} ",channelId);
		        	
		        	mergeNextMessage(msgIdx, idx, pipe, channelId);
			        			      
		        }	
				if (Pipe.hasContentToRead(pipe)) {
					writeToChannelBatchCountDown[idx] = 0;//send now nothing else is mergable
				}
		        		        
		        Pipe.releaseAllPendingReadLock(input[idx]);
		        
		        
		        if (ServerCoordinator.TEST_RECORDS) {
		        	ByteBuffer temp = workingBuffers[idx].duplicate();
		        	temp.flip();
		        	testValidContent(idx, temp);
		        }
		        
		      //  logger.info("total bytes written {} ",totalBytesWritten);
		        
		       // logger.info("write bytes {} for id {}",workingBuffers[idx].position(),channelId);
		        
		        workingBuffers[idx].flip();
	        } else {
	        	//logger.info("no server connection found for id:{}",channelId);
		        
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
		Pipe.readNextWithoutReleasingReadLock(input[idx]);
	}

	private boolean isNextMessageMergeable(Pipe<NetPayloadSchema> pipe, final int msgIdx, final int idx, final long channelId, boolean debug) {

		if (debug) {
		    logger.info("Data {} {} {} {} ",
		    		    Pipe.hasContentToRead(pipe),
		    		    Pipe.peekInt(pipe)==msgIdx,
		    		    workingBuffers[idx].remaining()>pipe.maxVarLen,
		    		    Pipe.peekLong(pipe, 1)==channelId	    		
		    		);
		}
		
		return  Pipe.hasContentToRead(pipe) && 
				Pipe.peekInt(pipe)==msgIdx && 
				workingBuffers[idx].remaining()>pipe.maxVarLen && 
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
				
				while (accumulators[idx].length() >= HTTPUtil.expectedOK.length()) {
					
				   int c = startsWith(accumulators[idx],HTTPUtil.expectedOK); 
				   if (c>0) {
					   
					   String remaining = accumulators[idx].substring(c*HTTPUtil.expectedOK.length());
					   accumulators[idx].setLength(0);
					   accumulators[idx].append(remaining);							    					   
					   
					   
				   } else {
					   logger.info("A"+Arrays.toString(HTTPUtil.expectedOK.getBytes()));
					   logger.info("B"+Arrays.toString(accumulators[idx].subSequence(0, HTTPUtil.expectedOK.length()).toString().getBytes()   ));
					   
					   logger.info("FORCE EXIT ERROR exlen {} BAD BYTE BUFFER at {}",HTTPUtil.expectedOK.length(),totalB);
					   System.out.println(accumulators[idx].subSequence(0, HTTPUtil.expectedOK.length()).toString());
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

    private boolean writeDataToChannel(int idx) {

    		boolean done = true;
    		if (!debugWithSlowWrites) {
		        try {
		        	assert(workingBuffers[idx].isDirect());

		        	ByteBuffer target = workingBuffers[idx];
		        	
		        	int bytesWritten = 0;
		        	do {		        		
		        		bytesWritten = writeToChannel[idx].write(target);	  
			        	if (bytesWritten>0) {
			        		totalBytesWritten+=bytesWritten;
			        	} else {
			        		break;
			        	}
			        	//output buffer may be too small so keep writing
		        	} while (target.hasRemaining());
		        	
		        	if (!target.hasRemaining()) {
		        		markDoneAndRelease(idx);
		        	} else {
		        		done = false;
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
			            
			            return false;
					}
				}

				if (expected!=0) {
					throw new UnsupportedOperationException();
				}
							
	        	if (!workingBuffers[idx].hasRemaining()) {
	        		markDoneAndRelease(idx);
	        	} else {
	        		done = false;
	        	}
    			
    		}
    		return done;
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
       
    	//System.err.println("done with connection");
    	workingBuffers[idx].clear();
    	
    	writeToChannel[idx]=null;
        int sequenceNo = 0;//not available here
        if (null!=releasePipe) {
        	Pipe.presumeRoomForWrite(releasePipe);
        	publishRelease(releasePipe, activeIds[idx],
        			       activeTails[idx]!=-1?activeTails[idx]: Pipe.tailPosition(input[idx]),
        					sequenceNo);
        }
        //logger.info("write is complete for {} ", activeIds[idx]);
        
        //beginSocketStart
       // System.err.println();
        //long now = System.nanoTime();
        
//        Appendables.appendNearestTimeUnit(System.err, now-ServerCoordinator.acceptConnectionStart);
//        System.err.append(" round trip for call\n");
//        
//        Appendables.appendNearestTimeUnit(System.err, now-ServerCoordinator.acceptConnectionRespond);
//        System.err.append(" round trip for data gathering\n");
        
        
        
//        long duration3 = System.nanoTime()-ServerCoordinator.newDotRequestStart;
//        Appendables.appendNearestTimeUnit(System.err, duration3);
//        System.err.append(" new dot trip for call\n");
//
//        long duration2 = System.nanoTime()-ServerCoordinator.orderSuperStart;
//        Appendables.appendNearestTimeUnit(System.err, duration2);
//        System.err.append(" super order trip for call\n");
        
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
