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
import com.ociweb.pronghorn.util.ServiceObjectHolder;

public class ServerSocketWriterStage extends PronghornStage {
    
    private static Logger logger = LoggerFactory.getLogger(ServerSocketWriterStage.class);
   
    private final Pipe<NetPayloadSchema>[] dataToSend;
    private final Pipe<ReleaseSchema> releasePipe;
    
    private final ServerCoordinator coordinator;
    private final int groupIdx;
    


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
    private long activeTails[];
    private long activeIds[]; 
    private int activeMessageIds[];    
  
    private long totalBytesWritten = 0;
    
    private int    activePipe = 0;
    
    private int bufferMultiplier = 4;


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
     * @param pipeIdx
     */
    public ServerSocketWriterStage(GraphManager graphManager, ServerCoordinator coordinator, Pipe<NetPayloadSchema>[] dataToSend, int pipeIdx) {
        super(graphManager, dataToSend, NONE);
        this.coordinator = coordinator;
        this.groupIdx = pipeIdx;
        this.dataToSend = dataToSend;
        this.releasePipe = null;
    }
    
    //optional ack mode for testing and other configuraitons..  
    
    public ServerSocketWriterStage(GraphManager graphManager, ServerCoordinator coordinator, Pipe<NetPayloadSchema>[] input, Pipe<ReleaseSchema> releasePipe, int groupIdx) {
        super(graphManager, input, releasePipe);
        this.coordinator = coordinator;
        this.groupIdx = groupIdx;
        this.dataToSend = input;
        this.releasePipe = releasePipe;
    }
    
    @Override
    public void startup() {
    	
    	int c = dataToSend.length;
    	writeToChannel = new SocketChannel[c];
    	workingBuffers = new ByteBuffer[c];
    	activeTails = new long[c];
    	activeIds = new long[c];
    	activeMessageIds = new int[c];
    	while (--c>=0) {    	
    		workingBuffers[c] = ByteBuffer.allocateDirect(bufferMultiplier*maxVarLength(dataToSend));    		
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
	    			if ( Pipe.hasContentToRead(dataToSend[x])) {
	    			//	logger.info("data to read for {}",x);
	    				didWork = true;
		            	int activeMessageId = Pipe.takeMsgIdx(dataToSend[x]);
		            			            	
		            	processMessage(activeMessageId, x);
		            	
		            	if (activeMessageId < 0) {
		            		return;
		            	}
	    			}// else {
	    			//	logger.info("no data to read {} {} ",x,dataToSend[x]);
	    			//}
	    		} else {
	    			
	    			//logger.info("write the channel");
	    			
	    			didWork = true;
	    			writeToChannel(x);    
	    			if (null != writeToChannel[x]) {
	    				
	    				//TODO: nice feature to add. however it never happens.
	    			//	logger.info("write was not completed so we have the opportunity to grow data??");
	    				
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
			writeToChannel(idx);
		
		} else if (NetPayloadSchema.MSG_DISCONNECT_203 == activeMessageId) {
		
			
			final long channelId = Pipe.takeLong(dataToSend[idx]);
			//logger.info("DISCONNECT MESSAGE FOUND BY SOCKET WRITER {} ",channelId);
			
		    Pipe.confirmLowLevelRead(dataToSend[idx], Pipe.sizeOf(dataToSend[idx], activeMessageId));
		    Pipe.releaseReadLock(dataToSend[idx]);
		    assert(Pipe.contentRemaining(dataToSend[idx])>=0);
		    
		    ServiceObjectHolder<ServerConnection> socketHolder = ServerCoordinator.getSocketChannelHolder(coordinator, groupIdx);
		    if (null!=socketHolder) {
		        ServerConnection serverConnection = socketHolder.get(channelId);	          
		        if (null!=serverConnection) {
		        	closeChannel(serverConnection.getSocketChannel()); 
		        }
		    }	                    
		    
		} else if (NetPayloadSchema.MSG_UPGRADE_207 == activeMessageId) {
			
			final long channelId = Pipe.takeLong(dataToSend[idx]);
			final int newRoute = Pipe.takeInt(dataToSend[idx]);
			
		    //set the pipe for any further communications
		   // ServerCoordinator.setTargetUpgradePipeIdx(coordinator, groupIdx, channelId, newRoute);
		    
		    Pipe.confirmLowLevelRead(dataToSend[idx], Pipe.sizeOf(dataToSend[idx], activeMessageId));
		    Pipe.releaseReadLock(dataToSend[idx]);
		    assert(Pipe.contentRemaining(dataToSend[idx])>=0);
		                      
		} else if (activeMessageId < 0) {
		    
			Pipe.confirmLowLevelRead(dataToSend[idx], Pipe.EOF_SIZE);
		    Pipe.releaseReadLock(dataToSend[idx]);
		    assert(Pipe.contentRemaining(dataToSend[idx])>=0);
		    
		    requestShutdown();	                    
		}
	}

	private final boolean enableWriteBatching = true;
	
    private void loadPayloadForXmit(final int msgIdx, final int idx) {
        
    	final boolean takeTail = NetPayloadSchema.MSG_PLAIN_210 == msgIdx;
    	final int msgSize = Pipe.sizeOf(dataToSend[idx], msgIdx);
    	
        Pipe<NetPayloadSchema> pipe = dataToSend[idx];
        final long channelId = Pipe.takeLong(pipe);
        
        activeIds[idx] = channelId;
        if (takeTail) {
        	activeTails[idx] =  Pipe.takeLong(pipe);
        } else {
        	activeTails[idx] = -1;
        }
        //byteVector is payload
        int meta = Pipe.takeRingByteMetaData(pipe); //for string and byte array
        int len = Pipe.takeRingByteLen(pipe);
        
        ServiceObjectHolder<ServerConnection> socketHolder = ServerCoordinator.getSocketChannelHolder(coordinator, groupIdx);
        
        if (null!=socketHolder) {
	        ServerConnection serverConnection = socketHolder.get(channelId);
	        	        
	        //only write if this connection is still valid
	        if (null != serverConnection) {        
				writeToChannel[idx] = serverConnection.getSocketChannel(); //ChannelId or SubscriptionId      
		        
		        //logger.debug("write {} to socket for id {}",len,channelId);
		        
		        ByteBuffer[] writeBuffs = Pipe.wrappedReadingBuffers(pipe, meta, len);
		        
		        workingBuffers[idx].clear();
		        workingBuffers[idx].put(writeBuffs[0]);
		        workingBuffers[idx].put(writeBuffs[1]);
		        		       		        
		        Pipe.confirmLowLevelRead(dataToSend[idx], msgSize);
		        Pipe.releaseReadLock(dataToSend[idx]);
		        
		        //In order to maximize throughput take all the messages which are gong to the same location.
		        
		        //if there is content and this content is also a message to send and we still have room in the working buffer and the channel is the same then we can batch it.
		        while (enableWriteBatching && Pipe.hasContentToRead(pipe) && 
		            Pipe.peekInt(pipe)==msgIdx && 
		            workingBuffers[idx].remaining()>pipe.maxAvgVarLen && 
		            Pipe.peekLong(pipe, 1)==channelId ) {
		        			        	
		        	//logger.trace("opportunity found to batch writes going to {} ",channelId);
		        	
		        	int m = Pipe.takeMsgIdx(pipe);
		        	assert(m==msgIdx): "internal error";
		        	long c = Pipe.takeLong(pipe);
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
		        		
			        Pipe.confirmLowLevelRead(pipe, msgSize);
			        Pipe.releaseReadLock(pipe);
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

    private void writeToChannel(int idx) {

    		
    		if (!debugWithSlowWrites) {
		        try {
		        	int bytesWritten = writeToChannel[idx].write(workingBuffers[idx]);	  
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
        
        if (null!=releasePipe) {
        	if (Pipe.hasRoomForWrite(releasePipe)) {
        		publishRelease(releasePipe, activeIds[idx], activeTails[idx]!=-1?activeTails[idx]: Pipe.tailPosition(dataToSend[idx]));
        	} else {
        		logger.info("potential hang from failure to release pipe");
        	}
        }
    }


	private static void publishRelease(Pipe<ReleaseSchema> pipe, long conId, long position) {
		assert(position!=-1);
		//logger.debug("sending release for {} at position {}",conId,position);
		
		int size = Pipe.addMsgIdx(pipe, ReleaseSchema.MSG_RELEASE_100);
		Pipe.addLongValue(conId, pipe);
		Pipe.addLongValue(position, pipe);
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
	}
}
