package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ServiceObjectHolder;

public class ServerSocketReaderStage extends PronghornStage {
    
    private static final int PLAIN_BOUNDARY_KNOWN = -1;

	private final int messageType;

	public static final Logger logger = LoggerFactory.getLogger(ServerSocketReaderStage.class);
    
    private final Pipe<NetPayloadSchema>[] output;
    private final Pipe<ReleaseSchema>[] releasePipes;
    private final ServerCoordinator coordinator;
    private final int groupIdx;
    
    private Selector selector;

    private int pendingSelections = 0;
    private final boolean isTLS;
    

    private ServiceObjectHolder<ServerConnection> holder;
    
    public static ServerSocketReaderStage newInstance(GraphManager graphManager, Pipe<ReleaseSchema>[] ack, Pipe<NetPayloadSchema>[] output, ServerCoordinator coordinator, int pipeIdx, boolean encrypted) {
        return new ServerSocketReaderStage(graphManager, ack, output, coordinator, pipeIdx, encrypted);
    }
    
    public ServerSocketReaderStage(GraphManager graphManager, Pipe<ReleaseSchema>[] ack, Pipe<NetPayloadSchema>[] output, ServerCoordinator coordinator, int groupIdx, boolean isTLS) {
        super(graphManager, ack, output);
        this.coordinator = coordinator;
        this.groupIdx = groupIdx;
        this.output = output;
        this.releasePipes = ack;
        this.isTLS = isTLS;
        this.messageType = isTLS ? NetPayloadSchema.MSG_ENCRYPTED_200 : NetPayloadSchema.MSG_PLAIN_210;
        coordinator.setStart(this);
    }

    @Override
    public void startup() {

        holder = ServerCoordinator.newSocketChannelHolder(coordinator, groupIdx);
                
        try {
            coordinator.registerSelector(groupIdx, selector = Selector.open());
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        //logger.debug("selector is registered for pipe {}",pipeIdx);
        
    }
    
    @Override
    public void shutdown() {
        int i = output.length;
        while (--i >= 0) {
        	Pipe.spinBlockForRoom(output[i], Pipe.EOF_SIZE);
            Pipe.publishEOF(output[i]);                
        }
        logger.warn("server reader has shut down");
    }

    private int maxWarningCount = 20;
    
    @Override
    public void run() {
        
    	releasePipesForUse();    	
    	
        ////////////////////////////////////////
        ///Read from socket
        ////////////////////////////////////////

        if (hasNewDataToRead()) {
        	
        	//logger.info("found new data to read on "+groupIdx);
            
            Iterator<SelectionKey>  keyIterator = selector.selectedKeys().iterator();   
            
            while (keyIterator.hasNext()) {                
                
            	
                SelectionKey selection = keyIterator.next();
                assert(0 != (SelectionKey.OP_READ & selection.readyOps())) : "only expected read"; 
                SocketChannel socketChannel = (SocketChannel)selection.channel();
           
                //logger.info("is blocking {} open {} ", selection.channel().isBlocking(),socketChannel.isOpen());
                
                
                //get the context object so we know what the channel identifier is
                ConnectionContext connectionContext = (ConnectionContext)selection.attachment();                
				long channelId = connectionContext.getChannelId();
                				
				
				if (isTLS) {
					
					SSLConnection cc = coordinator.get(channelId, groupIdx);
						
					if (null!=cc.getEngine()) {
						HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();
			    		
			    		if (false && handshakeStatus!=HandshakeStatus.FINISHED && handshakeStatus!=HandshakeStatus.NOT_HANDSHAKING) {
			    			assert(-1 == coordinator.checkForResponsePipeLineIdx(cc.getId())) : "locked in on pipe and we require "+handshakeStatus;
			    		}
						
			    		 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
				                Runnable task;//TODO: there is anopporuntity to have this done by a different stage in the future.
				                while ((task = cc.getEngine().getDelegatedTask()) != null) {
				                	task.run();
				                }
				                handshakeStatus = cc.getEngine().getHandshakeStatus();
						 } else if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
							 releasePipesForUse();
							 assert(-1 == coordinator.checkForResponsePipeLineIdx(cc.getId())) : "should have already been relased";	
							 continue;//one of the other pipes can do work
						 }
					}
				}
					
				releasePipesForUse();
				int responsePipeLineIdx = coordinator.responsePipeLineIdx(channelId);
				if (-1 == responsePipeLineIdx) { //handshake is dropped by input buffer at these loads?
					releasePipesForUse();
					responsePipeLineIdx = coordinator.responsePipeLineIdx(channelId);
					if (responsePipeLineIdx<0) {
//		    			if (--maxWarningCount>0) {//this should not be a common error but needs to be here to promote good configurations
		    				logger.warn("bump up maxPartialResponsesServer count, performance is slowed due to waiting for available input pipe on client");
//		    			}
						continue;//try other connections which may already have pipes, this one can not reserve a pipe now.
					}
				}
								
				Pipe<NetPayloadSchema> targetPipe = output[responsePipeLineIdx]; //TODO: add support for groupIdx here? each group needs its own pool....  
				int pumpState = pumpByteChannelIntoPipe(socketChannel, channelId, targetPipe); 
                if (pumpState<0) {//consumes from channel until it has no more or pipe has no more room
                	//pipe full do again later
                	
//	    			if (--maxWarningCount>0) {//this should not be a common error but needs to be here to promote good configurations
//	    				logger.warn("pipe full go again later");
//	    			}
                	
                	return;
                } else if (pumpState>0) {
                	assert(1==pumpState) : "Can only remove if all the data is known to be consumed";
                	keyIterator.remove();//only remove if we have consumed the data !!
                	pendingSelections--;
                }
                releasePipesForUse();
            }
        }
    }

	private void releasePipesForUse() {
		int i = releasePipes.length;
		while (--i>=0) {
			Pipe<ReleaseSchema> a = releasePipes[i];
			while (Pipe.hasContentToRead(a)) {
	    						
	    		int id = Pipe.takeMsgIdx(a);
	    		if (id == ReleaseSchema.MSG_RELEASE_100) {
	    			long idToClear = Pipe.takeLong(a); //PipeReader.readLong(a, NetParseAckSchema.MSG_PARSEACK_100_FIELD_CONNECTIONID_1);
	    			long pos = Pipe.takeLong(a);//PipeReader.readLong(a, NetParseAckSchema.MSG_PARSEACK_100_FIELD_POSITION_2);
	    			
	    			int pipeIdx = coordinator.checkForResponsePipeLineIdx(idToClear);
	    			
	    			///////////////////////////////////////////////////
	    			//if sent tail matches the current head then this pipe has nothing in flight and can be re-assigned
	    			if (pipeIdx>=0 && (Pipe.headPosition(output[pipeIdx]) == pos)) {
	    				coordinator.releaseResponsePipeLineIdx(idToClear);
	    				//logger.info("did release for {}",idToClear);

	    		    //TODO: these release ofen before opening a new oen must check priorty for this connection...		
	    				
	    			} else {
	    				//logger.info("no release for {}",idToClear); //what about EOF is that blocking the relase.
	    				
	    				if (pipeIdx>=0) {
	    					if (pos>Pipe.headPosition(output[pipeIdx])) {
	    						System.err.println("EEEEEEEEEEEEEEEEEEEEEEEEe  got ack but did not release on server. pipe "+pipeIdx+" pos "+pos+" expected "+Pipe.headPosition(output[pipeIdx]) );
	    						//System.exit(-1);
	    					} else {
	    						//this is the expected case where more data came in for this pipe
	    					}
	    				}
	    			}
	    			Pipe.confirmLowLevelRead(a, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASE_100));
	    		} else {
	    			assert(-1==id);
	    			Pipe.confirmLowLevelRead(a, Pipe.EOF_SIZE);
	    		}
	    		Pipe.releaseReadLock(a);	    		
	    	}
		}
	}

    private boolean hasNewDataToRead() {
    	
    	if (pendingSelections>0) {
    		return true;
    	}
    	
        try {        	        	
        	/////////////
        	//CAUTION - select now clears pevious count and only returns the additional I/O opeation counts which have become avail since the last time SelectNow was called
        	////////////        	
            pendingSelections=selector.selectNow();
      //      logger.info("pending new selections {} ",pendingSelections);
            return pendingSelections>0;
        } catch (IOException e) {
            logger.error("unexpected shutdown, Selector for this group of connections has crashed with ",e);
            requestShutdown();
            return false;
        }
    }
    
    //returns -1 for did not start, 0 for started, and 1 for finished all.
    public int pumpByteChannelIntoPipe(SocketChannel sourceChannel, long channelId, Pipe<NetPayloadSchema> targetPipe) {
    	
        //keep appending messages until the channel is empty or the pipe is full
        if (Pipe.hasRoomForWrite(targetPipe)) {          
        	//logger.info("pump block for {} ",channelId);
            try {                
                
                long len=0;//if data is read then we build a record around it
                //NOTE: the byte buffer is no longer than the valid maximum length but may be shorter based on end of wrap arround
                ByteBuffer[] b = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(targetPipe), targetPipe);
                       
                //TODO: URGENT needs to keep write open while running in this loop then do a single publish flush if possible. small writes are bad clogging the system.
                long temp = 0;
                do {
                	temp = sourceChannel.read(b);
                	if (temp>0){
                		len+=temp;
                	}            
           //     	System.err.println(temp+"  for channel "+channelId); //this seems to show that this loop is not needed because the data is already grouped?? BUT could do larger groups going arround.
                } while (temp>0);
                      
                return publishOrAbandon(channelId, targetPipe, len, b, temp>=0);

            } catch (IOException e) {
            	
            		this.coordinator.releaseResponsePipeLineIdx(channelId);
            	
                    recordErrorAndClose(sourceChannel, e);
                        
                    return -1;
            }
        } else {
//        	try {
//        		Thread.yield();
//				Thread.sleep(10000);
//			} catch (InterruptedException e) {
//			}
//        	
////        	logger.info("no room to write on server for {}, check that data is getting released or that larger blocks are written here {} ",channelId, targetPipe);
////        	
////       		logger.error("FORCED EXIT");
////       		System.exit(-1);
////        	
        	return -1;
        }
    }

	private int publishOrAbandon(long channelId, Pipe<NetPayloadSchema> targetPipe, long len, ByteBuffer[] b, boolean isOpen) {
		if (len>0) {
			boolean fullTarget = b[0].remaining()==0 && b[1].remaining()==0;                	
			publishData(targetPipe, channelId, len);                  	 
			return (fullTarget&&isOpen) ? 0 : 1; //only for 1 can we be sure we read all the data
		} else {
			 Pipe.unstoreBlobWorkingHeadPosition(targetPipe);//we did not use or need the writing buffers above.
			 return 1;//yes we are done
		}
	}

    private void recordErrorAndClose(ReadableByteChannel sourceChannel, IOException e) {
       //   logger.error("unable to read",e);
          //may have been closed while reading so stop
          if (null!=sourceChannel) {
              try {
                  sourceChannel.close();
               } catch (IOException e1) {
                   logger.warn("unable to close channel",e1);
               }
              
          }
    }

    private void publishData(Pipe<NetPayloadSchema> targetPipe, long channelId, long len) {

    	assert(len<Integer.MAX_VALUE) : "Error: blocks larger than 2GB are not yet supported";
        
        int size = Pipe.addMsgIdx(targetPipe,messageType);               
        Pipe.addLongValue(channelId, targetPipe);  

        if (NetPayloadSchema.MSG_PLAIN_210 == messageType) {
        	Pipe.addLongValue(-1, targetPipe);
        }

        
        int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(targetPipe);

//ONLY VALID FOR UTF8
//        boolean showRequests = true;
//        if (showRequests) {
//        	logger.info("//////////////////Server read for channel {} \n{}\n/////////////////////",channelId, Appendables.appendUTF8(new StringBuilder(), targetPipe.blobRing, originalBlobPosition, (int)len, targetPipe.blobMask));               
//        }
        
        
        Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int)len, targetPipe);  
        
        //all breaks are detected by the router not here
        //(section 4.1 of RFC 2616) end of header is \r\n\r\n but some may only send \n\n
        //
   
        Pipe.confirmLowLevelWrite(targetPipe, size);
        Pipe.publishWrites(targetPipe);
        
    }
    
}
