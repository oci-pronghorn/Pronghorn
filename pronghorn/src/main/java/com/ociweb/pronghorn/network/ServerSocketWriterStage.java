package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.PipeWorkWatcher;
import com.ociweb.pronghorn.util.ServiceObjectHolder;

/**
 * Server-side stage that writes back to the socket. Useful for building a server.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ServerSocketWriterStage extends PronghornStage {
    
    private static Logger logger = LoggerFactory.getLogger(ServerSocketWriterStage.class);
    public static boolean showWrites = false;
    //must work with only 16 in flight!!
 	public static long hardLimtNS = 20_000L;//20 micros -- must be fast enough for the telemetry set now to 100ms
    //also note however data can be written earlier if:
	//   1. the buffer has run out of space 
	//   2. if the pipe has no more data.
    	
    private final Pipe<NetPayloadSchema>[] input;
    private final Pipe<ReleaseSchema> releasePipe;
    
    private final ServerCoordinator coordinator;
    
    //TODO: may want multiple buffers in the future per pipe...
    private ByteBuffer    workingBuffers[];
    private boolean       bufferChecked[];
    private SocketChannel writeToChannel[];
    private long          writeToChannelId[];
    private int           writeToChannelMsg[];
    private int           writeToChannelBatchCountDown[]; 
    
    private long activeTails[];
    private long activeIds[]; 
    private int activeMessageIds[];    

    private int maxBatchCount;

	private static final boolean enableWriteBatching = true;
    

	private final boolean debugWithSlowWrites = false;// false; //TODO: set from coordinator, NOTE: this is a critical piece of the tests
	private final int debugMaxBlockSize = 7;//50000;
	
	private GraphManager graphManager;
	    
    private final PipeWorkWatcher pww = new PipeWorkWatcher();
    private boolean[] pendingWrites;
      
    
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
     * @param dataToSend _in_ The data to be written to the socket.
     */
    public ServerSocketWriterStage(GraphManager graphManager, ServerCoordinator coordinator, Pipe<NetPayloadSchema>[] dataToSend) {
        super(graphManager, dataToSend, NONE);
        this.coordinator = coordinator;
        this.input = dataToSend;
        this.releasePipe = null;
     
        this.graphManager = graphManager;

        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
        GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
        //for high volume this must be on its own
        GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
        //TODO: can we run all these on a single thread with writer/or across??
    
        //NOTE:more writers are bad and slow us down but we need some...
        
      // GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE,  40_000L, this);
        
        Number dsr = graphManager.defaultScheduleRate();
        if (dsr!=null) {
        	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, dsr.longValue()/2, this);
        }

      
    }
    
    
    //optional ack mode for testing and other configuraitons..  
    
    public ServerSocketWriterStage(GraphManager graphManager, ServerCoordinator coordinator, Pipe<NetPayloadSchema>[] input, Pipe<ReleaseSchema> releasePipe) {
        super(graphManager, input, releasePipe);
        this.coordinator = coordinator;
        this.input = input;
        this.releasePipe = releasePipe;
          
        
        this.graphManager = graphManager;
       
        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
        GraphManager.addNota(graphManager, GraphManager.LOAD_MERGE, GraphManager.LOAD_MERGE, this);
        //for high volume this must be on its own
        GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
          
    }
    
    @Override
    public void startup() {
    	
    	final Number rate = (Number)GraphManager.getNota(graphManager, this, GraphManager.SCHEDULE_RATE, null);
    	    	
    	this.maxBatchCount = Math.max(1, ( null==rate ? 16 : (int)(hardLimtNS/rate.longValue()))); 
    	
    	//logger.info("server socket write batch count "+maxBatchCount+" cycle rate "+rate.longValue()); // 100_000;
    	
    	int c = input.length;

    	writeToChannel = new SocketChannel[c];
    	writeToChannelId = new long[c];
    	writeToChannelMsg = new int[c];
    	writeToChannelBatchCountDown = new int[c];
    	
    	workingBuffers = new ByteBuffer[c];
    	bufferChecked = new boolean[c];
    	activeTails = new long[c];
    	activeIds = new long[c];
    	activeMessageIds = new int[c];
    	Arrays.fill(activeTails, -1); 

        pww.init(input);
        pendingWrites = new boolean[pww.groups()];
        //Arrays.fill(pendingWrites, true);
    }
    
    @Override
    public void shutdown() {

    }


    @Override
    public void run() {

    	if (PipeWorkWatcher.hasWork(pww)) {
    
    		this.didWorkMonitor.published();    	
    		
	    	do {
		    	int g = pww.groups();
				while (--g >= 0) {
								
					final int version = PipeWorkWatcher.version(pww, g);
					if (pendingWrites[g] || PipeWorkWatcher.hasWork(pww,g)) {
						   				
						boolean done = true;
						int start = PipeWorkWatcher.getStartIdx(pww, g);
						int limit = PipeWorkWatcher.getLimitIdx(pww, g);
						boolean hasPendingWritesInGroup = false;
						
						for(int x = start; x<limit; x++) {
													
							
					    	boolean doingWork = false;						    				    	    		
				    		//ensure all writes are complete
				    		if (null == writeToChannel[x]) {	    	
				    			
								//second check for full content is critical or the data gets copied too soon
						
								if (!Pipe.hasContentToRead(input[x])) {    				
									//no content to read on the pipe
									//all the old data has been written so the writeChannel remains null
									continue;
								} else {
									
									processMessage(Pipe.takeMsgIdx(input[x]), x);
									doingWork = true;
														
								}
				    		} else {	    
				    			doingWork = publishDataFromLastPass(doingWork, 0, x);
				    		
				    		}
					
				    		//do any more work we find in the pipe.
				    		while ((null == writeToChannel[x]) && Pipe.hasContentToRead(input[x])) {
				    			processMessage(Pipe.takeMsgIdx(input[x]), x);
								doingWork = true;					    			
				    		}
				    									    
							//only set if we do not have any waiting data..
							if (null == writeToChannel[x]) { 
								//only clear when this data is published.
								PipeWorkWatcher.setTailPos(pww, x, Pipe.getWorkingTailPosition(input[x]));
								
								if (!Pipe.isEmpty(input[x])) {
									done = false;
								}
								
							} else {
								done = false;
								//must come back 
								hasPendingWritesInGroup = true;							
							}
					  				
						}
						
						pendingWrites[g] = hasPendingWritesInGroup;
						
						if (done) {
							PipeWorkWatcher.clearScanState(pww, g, version);
						}					
						
						if (hasPendingWritesInGroup) {
							return;
						}
						
					}
				}
				
	    	} while (PipeWorkWatcher.hasWork(pww));
		
    	}
    	
    }


	private boolean publishDataFromLastPass(boolean doingWork, int iteration, int x) {
		Pipe<NetPayloadSchema> localInput = input[x];
 
		ByteBuffer localWorkingBuffer = workingBuffers[x];
		if (null!=localWorkingBuffer) {
			int capacity = localWorkingBuffer.capacity();
			int limit = localWorkingBuffer.limit();
			boolean hasRoomToWrite = capacity-limit > localInput.maxVarLen;
			//note writeToChannelBatchCountDown is set to zero when nothing else can be combined...
			if (
				//these are set up to minimize writes so we can write bigger blocks at once.	
				/// 	
			    //accumulating too long so flush now.
					((iteration>1 && writeToChannelBatchCountDown[x]<=0) || --writeToChannelBatchCountDown[x]<=0) //only count on first pass since it is time based.  
				||
					!hasRoomToWrite //must write to network out buffer has no more room
				||
				    //if we have less than 1/2 of blob used wait for normal count down above
				    //if above this and we have no new data go ahead and write.
					( (limit>= (localInput.sizeOfBlobRing>>1) ) &&  Pipe.isEmpty(localInput) ) //for low latency when pipe is empty fire now...
				) {
							
				writeToChannelBatchCountDown[x]=-4;
				writeToChannelMsg[x] = -1;
				
				if (!(doingWork = writeDataToChannel(x))) {
					//this channel did not write but we need to check the others		    		
				}	
			} else {
				
				//must set to true to ensure we count up the iterations above.
				doingWork |= (!Pipe.hasContentToRead(localInput));
				
				
				//unflip
				int p = ((Buffer)localWorkingBuffer).limit();
				((Buffer)localWorkingBuffer).limit(localWorkingBuffer.capacity());
				((Buffer)localWorkingBuffer).position(p);	    				
				
				int h = 0;
				while (	isNextMessageMergeable(localInput, writeToChannelMsg[x], x, writeToChannelId[x], false) ) {
					h++;
					//logger.info("opportunity found to batch writes going to {} ", writeToChannelId[x]);
						    					
					mergeNextMessage(writeToChannelMsg[x], x, localInput, writeToChannelId[x]);
						    					
				}	
				
				if (h>0) {
					Pipe.releaseAllPendingReadLock(localInput);
				}
				((Buffer)localWorkingBuffer).flip();
	
			}
		}
		
		
		return doingWork;
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
		    ServiceObjectHolder<ServerConnection> socketHolder = ServerCoordinator.getSocketChannelHolder(coordinator);
    
		    if (null!=socketHolder) {
		    	//logger.info("removed server id {}",channelId);
                //new Exception("removed server id "+channelId).printStackTrace();
                
		    	//we are disconnecting so we will remove the connection from the holder.
		        ServerConnection serverConnection = socketHolder.remove(channelId);	          
		     
		        if (null != serverConnection) {
		        	//do not close since it is still known to sequence.
		        	serverConnection.decompose();
		        }
		    }	     
	   
		    
		    
		} else if (NetPayloadSchema.MSG_UPGRADE_307 == activeMessageId) {
			
			//set the pipe for any further communications
		    long channelId = Pipe.takeLong(input[idx]);
			int pipeIdx = Pipe.takeInt(input[idx]);
						
			ServerCoordinator.setUpgradePipe(coordinator, 
		    		channelId, //connection Id 
		    		pipeIdx); //pipe idx
		    
		    //switch to new reserved connection?? after upgrade no need to use http router
		    //perhaps? 	coordinator.releaseResponsePipeLineIdx(channelId);
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
        
    	final int msgSize = Pipe.sizeOf(input[idx], msgIdx);
    	
        Pipe<NetPayloadSchema> pipe = input[idx];
        long channelId = Pipe.takeLong(pipe);
        final long arrivalTime = Pipe.takeLong(pipe);        
               
        activeIds[idx] = channelId;
        if (NetPayloadSchema.MSG_PLAIN_210 == msgIdx) {
        	activeTails[idx] = Pipe.takeLong(pipe);
        } else {
        	assert(msgIdx == NetPayloadSchema.MSG_ENCRYPTED_200);
        	activeTails[idx] = -1;
        }
        //byteVector is payload
        int meta = Pipe.takeByteArrayMetaData(pipe); //for string and byte array
        int len = Pipe.takeByteArrayLength(pipe);
                
        assert(len>0) : "All socket writes must be of zero length or they should not be requested";
    
        prepSocketConnection(msgIdx, idx, msgSize, pipe, channelId, meta, len);
                
    }


	private void prepSocketConnection(final int msgIdx, final int idx, final int msgSize, Pipe<NetPayloadSchema> pipe,
			long channelId, int meta, int len) {
		
		ServiceObjectHolder<ServerConnection> socketHolder = ServerCoordinator.getSocketChannelHolder(coordinator);
        assert(null!=socketHolder) : "Internal error, too early";
        ServerConnection serverConnection = ServiceObjectHolder.get(socketHolder, channelId);
        	        
        //only write if this connection is still valid
        if (null != serverConnection) {        
        	channelId = serverConnection.id;
        	
        	if (showWrites) {	        	
        		debugShowWrites(pipe, meta, len);
        	}	        	
        	
        	writeToChannel[idx] = BaseConnection.getSocketChannel(serverConnection); //ChannelId or SubscriptionId   
			writeToChannelId[idx] = channelId;
			writeToChannelMsg[idx] = msgIdx;
			writeToChannelBatchCountDown[idx] = maxBatchCount;
			
			prepBuffer(msgIdx, idx, msgSize, pipe, channelId, meta, len);
        } else {
        	//logger.info("\nno server connection found for id:{} droped bytes",channelId);
	        
	        Pipe.confirmLowLevelRead(pipe, msgSize);
	        Pipe.releaseReadLock(pipe);
        }
	}


	private void debugShowWrites(Pipe<NetPayloadSchema> pipe, int meta, int len) {
		//Do not report telemetry calls... show show up as monitor
		if (!this.isMonitor() ) {
		
			int pos = Pipe.convertToPosition(meta, pipe);
			logger.info("/////////len{}///////////\n"+
					Appendables.appendUTF8(new StringBuilder(), Pipe.blob(pipe), pos, len, Pipe.blobMask(pipe))
			+"\n////////////////////",len);
		}
	}


	private void prepBuffer(final int msgIdx, final int idx, final int msgSize, Pipe<NetPayloadSchema> pipe,
			long channelId, int meta, int len) {
		
		ByteBuffer[] writeBuffs = Pipe.wrappedReadingDirectBuffers(pipe, meta, len);
		
		//only write if we do not think there is anything to "roll up"
//		if ((!Pipe.hasContentToRead(pipe))
//			|| 	Pipe.peekLong(pipe, 1)!=channelId
//			||  Pipe.peekInt(pipe)!=msgIdx			
//				) {

			
//			try {
//				Selector s = Selector.open();
//				//s.provider().openServerSocketChannel().socket().
//				
//				SelectionKey reg = writeToChannel[idx].register(s, SelectionKey.OP_WRITE);
//				
//				if (s.selectNow()>=0) {	//hacktest here.

            
//            //need to use this in 10 
//			try {
//				writeToChannel[idx].setOption(ExtendedSocketOptions.TCP_QUICKACK, Boolean.TRUE);
//			} catch (IOException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}	
			
			//////////////////////////////
					try {//try immediate write first then store if we must
						if (writeToChannel[idx].write(writeBuffs,0,2)==len) {
							
							//all wrote so clear
							markDoneAndRelease(idx);
							Pipe.confirmLowLevelRead(pipe, msgSize);		        
							Pipe.releaseReadLock(pipe);
							return;
							
						}			
					} catch (IOException e) {
						//ignore we will try again after the wait.
						logger.trace("error attempting to write",e);
					}
			///////////////////////////
					
//					s.selectedKeys().removeAll(s.selectedKeys());
//					
//					s.close();
//				}
//				
////				
//			} catch (IOException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
//			
			
			
//		}
		
		//only need buffer after this point so only do this work here so it happens less often
		if (bufferChecked[idx]) {
		} else {
			buildBuffers(idx, pipe, writeToChannel[idx]);
		}

		rollUpOrSlowPath(msgIdx, idx, msgSize, pipe, channelId, writeBuffs);
	}


	private void rollUpOrSlowPath(final int msgIdx, final int idx, final int msgSize, Pipe<NetPayloadSchema> pipe,
			long channelId, ByteBuffer[] writeBuffs) {
		ByteBuffer directBuffer = workingBuffers[idx];
		((Buffer)directBuffer).clear();
		directBuffer.put(writeBuffs[0]);
		directBuffer.put(writeBuffs[1]);
				
		assert(!writeBuffs[0].hasRemaining());
		assert(!writeBuffs[1].hasRemaining());
				       		        
		Pipe.confirmLowLevelRead(pipe, msgSize);		        
		Pipe.releaseReadLock(pipe);
		
		combineWritesIfPossible(msgIdx, idx, pipe, channelId);

		
     //  logger.info("total bytes written {} ",totalBytesWritten);
		
      // logger.info("write bytes {} for id {}",workingBuffers[idx].position(),channelId);
		
		((Buffer)directBuffer).flip();
	}


	private void combineWritesIfPossible(final int msgIdx, final int idx, Pipe<NetPayloadSchema> pipe, long channelId) {
		//In order to maximize throughput take all the messages which are gong to the same location.

		//if there is content and this content is also a message to send and we still have room in the working buffer and the channel is the same then we can batch it.
		while (enableWriteBatching && isNextMessageMergeable(pipe, msgIdx, idx, channelId, false) ) {		        			        	
			//logger.trace("opportunity found to batch writes going to {} ",channelId);
			
			mergeNextMessage(msgIdx, idx, pipe, channelId);
		    			      
		}
			        		        
		Pipe.releaseAllPendingReadLock(input[idx]);
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
		int meta2 = Pipe.takeByteArrayMetaData(pipe); //for string and byte array
		int len2 = Pipe.takeByteArrayLength(pipe);
		
		
    	if (showWrites) {
        	
    		debugShowWrites(pipe, meta2, len2);
    	}
    	
		ByteBuffer[] writeBuffs2 = Pipe.wrappedReadingBuffers(pipe, meta2, len2);
		
		workingBuffers[idx].put(writeBuffs2[0]);
	
		if (writeBuffs2[1].hasRemaining()) {
			workingBuffers[idx].put(writeBuffs2[1]);
		}
		
		assert(!writeBuffs2[0].hasRemaining());
		assert(!writeBuffs2[1].hasRemaining());
				        		
		Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(NetPayloadSchema.instance, msgIdx));
		//Pipe.readNextWithoutReleasingReadLock(input[idx]);
		Pipe.releaseReadLock(input[idx]);
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


		if (Pipe.hasContentToRead(pipe) ) {
			if (Pipe.peekLong(pipe, 1)==channelId 
				&& Pipe.peekInt(pipe)==msgIdx
				&& workingBuffers[idx].remaining()>pipe.maxVarLen) {
				return true;
			} else {
				//not for same channel or message or out of room so we must flush now.
				writeToChannelBatchCountDown[idx] = -2;
//							if (Pipe.peekInt(pipe)!=msgIdx) {
//								System.out.println("not msgIdx matching");
//							}
//							if (workingBuffers[idx].remaining()<=pipe.maxVarLen) {
//								System.out.println("no room to write needs :"+pipe.maxVarLen+" has "+workingBuffers[idx].remaining());
//							}				
//							if (Pipe.peekLong(pipe, 1)!=channelId) { //by far most common here.
//								System.out.println("stop accumulation: "+channelId+" vs "+Pipe.peekLong(pipe, 1));
//							}
							
				//TODO: if we have multiple blocks per pipe we could group them by connection Id for more effective writes..			
							
				return false;
			}
		} else {
	//		System.out.println("no content to read "+pipe);
			return false;
		}
	
	}
    
	private void buildBuffers(int i, Pipe<NetPayloadSchema> pipe, SocketChannel socketChannel) {
		try {
			
			int minBufSize = Math.max(workingBuffers.length>8 ? pipe.maxVarLen*4 : pipe.sizeOfBlobRing, 
					         socketChannel.getOption(StandardSocketOptions.SO_SNDBUF));
								
			if (null==workingBuffers[i] || workingBuffers[i].capacity()<minBufSize) {
				workingBuffers[i] = ByteBuffer.allocateDirect(minBufSize);
			}
			bufferChecked[i] = true;
		} catch (ClosedChannelException cce) {
			if (null==workingBuffers[i]) {
				workingBuffers[i] = ByteBuffer.allocateDirect(workingBuffers.length>8 ? pipe.maxVarLen*4 : pipe.sizeOfBlobRing);
			}
			bufferChecked[i] = true;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

    private boolean writeDataToChannel(int idx) {
    		return (!debugWithSlowWrites) 
    				? writeDataToChannelQuick(idx, true)
    			    : writeToDataChannelDebug(idx, true);   			    		
    }


	private boolean writeDataToChannelQuick(int idx, boolean done) {
		try {
			
			ByteBuffer source = workingBuffers[idx];
			assert(source.isDirect());
			
			int bytesWritten = 0;
  
			int localWritten = 0;
		//	do {		 
				
//				int v =  writeToChannel[idx].validOps();
//				
//				
//				if (0== (SelectionKey.OP_WRITE&v)) {
//					System.out.println("found itsssss ");
//					break;//leave if we can not write.
//				}
//				
				
				bytesWritten = writeToChannel[idx].write(source);	
			
		    	if (bytesWritten>0) {
		    		localWritten += bytesWritten;
		    	} else {
		    	//	break;
		    	}
		    
		    	//output buffer may be too small so keep writing
		//	} while (source.hasRemaining());
			 
			// max 157569   260442
	//System.out.println("single block write: "+localWritten+" bytes, has rem:"+target.hasRemaining()+" capacity:"+target.capacity()); //  179,670
			
			if (!source.hasRemaining()) {
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
		return done;
	}


	private boolean writeToDataChannelDebug(int idx, boolean done) {
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
				}
				
				if (!this.isMonitor() ) {
					System.out.println("wrote bytes "+len);
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
    	
    	writeToChannel[idx]=null;
        int sequenceNo = 0;//not available here
        if (null!=releasePipe) {
        	Pipe.presumeRoomForWrite(releasePipe);
        	publishRelease(releasePipe, activeIds[idx],
        			       activeTails[idx]!=-1?activeTails[idx]: Pipe.tailPosition(input[idx]),
        					sequenceNo);
        }
    }
   

	private static void publishRelease(Pipe<ReleaseSchema> pipe, long conId, long position, int sequenceNo) {
		assert(position!=-1);
		//logger.debug("sending release for {} at position {}",conId,position);
		
		int size = Pipe.addMsgIdx(pipe, ReleaseSchema.MSG_RELEASEWITHSEQ_101);
		Pipe.addLongValue(conId, pipe);
		Pipe.addLongValue(position, pipe);
		Pipe.addIntValue(sequenceNo, pipe);
		Pipe.addIntValue(-1, pipe);
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
				
	}
}
