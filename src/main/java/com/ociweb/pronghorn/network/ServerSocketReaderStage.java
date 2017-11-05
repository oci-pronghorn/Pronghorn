package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Consumer;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

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

public class ServerSocketReaderStage extends PronghornStage {
   
	private final int messageType;

	public static final Logger logger = LoggerFactory.getLogger(ServerSocketReaderStage.class);
    
    private final Pipe<NetPayloadSchema>[] output;
    private final Pipe<ReleaseSchema>[] releasePipes;
    private final ServerCoordinator coordinator;

    private Selector selector;

    private int pendingSelections = 0;
    
    public static boolean showRequests = false;
    
    private StringBuilder[] accumulators;
    private boolean shutdownInProgress;
    private final String label;
    private ArrayList<SelectionKey> doneSelectors = new ArrayList<SelectionKey>(100);

    
    public static ServerSocketReaderStage newInstance(GraphManager graphManager, Pipe<ReleaseSchema>[] ack, Pipe<NetPayloadSchema>[] output, ServerCoordinator coordinator, boolean encrypted) {
        return new ServerSocketReaderStage(graphManager, ack, output, coordinator);
    }
    
    public ServerSocketReaderStage(GraphManager graphManager, Pipe<ReleaseSchema>[] ack, Pipe<NetPayloadSchema>[] output, ServerCoordinator coordinator) {
        super(graphManager, ack, output);
        this.coordinator = coordinator;

        this.label = "\n"+coordinator.host()+":"+coordinator.port()+"\n";
        
        this.output = output;
        this.releasePipes = ack;

        this.messageType = coordinator.isTLS ? NetPayloadSchema.MSG_ENCRYPTED_200 : NetPayloadSchema.MSG_PLAIN_210;
        coordinator.setStart(this);
        
        GraphManager.addNota(graphManager, GraphManager.PRODUCER, GraphManager.PRODUCER, this);
        GraphManager.addNota(graphManager, GraphManager.LOAD_BALANCER, GraphManager.LOAD_BALANCER, this);
        
        
    }
        
    @Override
    public String toString() {
    	return super.toString()+label;
    }

    @Override
    public void startup() {

    	if (ServerCoordinator.TEST_RECORDS) {
			int i = output.length;
			accumulators = new StringBuilder[i];
			while (--i >= 0) {
				accumulators[i]=new StringBuilder();					
			}
    	}
		
        ServerCoordinator.newSocketChannelHolder(coordinator);
                
        try {
            coordinator.registerSelector(selector = Selector.open());
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        //logger.debug("selector is registered for pipe {}",pipeIdx);
        
        
    }
    
    @Override
    public void shutdown() {
    	Pipe.publishEOF(output);  
       
        logger.trace("server reader has shut down");
    }

    
    private final Consumer<SelectionKey> selectionKeyAction = new Consumer<SelectionKey>(){
			@Override
			public void accept(SelectionKey selection) {
				processSelection(selection); 
			}
    };    
    
    private long connectionIdToRemove = -1;
    private final Consumer<SelectionKey> selectionKeyRemoval = new Consumer<SelectionKey>(){
			@Override
			public void accept(SelectionKey selection) {
				
				if ((connectionIdToRemove == ((ConnectionContext)selection.attachment()).getChannelId())
					&& (!doneSelectors.contains(selection))) {
						removeSelection(selection);		
				}
			}
    };  
    
    @Override
    public void run() {

    	 if(shutdownInProgress) {
	    	 int i = output.length;
	         while (--i >= 0) {
	         	if (null!=output[i] && Pipe.isInit(output[i])) {
	         		if (!Pipe.hasRoomForWrite(output[i], Pipe.EOF_SIZE)){ 
	         			return;
	         		}  
	         	}
	         }
	         requestShutdown();
	         return;
    	 }
    	
        ////////////////////////////////////////
        ///Read from socket
        ////////////////////////////////////////

	    //max cycles before we take a break.
    	int maxIterations = 100; //important or this stage will take all the resources.
    	
        while (--maxIterations>=0 & hasNewDataToRead()) { //single & to ensure we check has new data to read.
        	
        	//logger.info("found new data to read on "+groupIdx);
            
           Set<SelectionKey> selectedKeys = selector.selectedKeys();
            
           assert(selectedKeys.size()>0);	            

           doneSelectors.clear();
	
           selectedKeys.forEach(selectionKeyAction);
                 
		   removeDoneKeys(selectedKeys);
		        	 
        }	        
    	
    }

	private void removeDoneKeys(Set<SelectionKey> selectedKeys) {
		//sad but this is the best way to remove these without allocating a new iterator
		// the selectedKeys.removeAll(doneSelectors); will produce garbage upon every call
		int c = doneSelectors.size();
		while (--c>=0) {
		    		selectedKeys.remove(doneSelectors.get(c));
		}
		
	}

	private void processSelection(SelectionKey selection) {
		assert(0 != (SelectionKey.OP_READ & selection.readyOps())) : "only expected read"; 
		SocketChannel socketChannel = (SocketChannel)selection.channel();
         
		//logger.info("is blocking {} open {} ", selection.channel().isBlocking(),socketChannel.isOpen());
		
		
		//get the context object so we know what the channel identifier is
		ConnectionContext connectionContext = (ConnectionContext)selection.attachment();                
		final long channelId = connectionContext.getChannelId();
						
		//logger.info("new request on connection {} ",channelId);
		
		SSLConnection cc = coordinator.connectionForSessionId(channelId);
		boolean processWork = true;
		if (coordinator.isTLS) {
				
			if (null!=cc && null!=cc.getEngine()) {
				HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();

				 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
		                Runnable task;//TODO: there is an opportunity to have this done by a different stage in the future.
		                while ((task = cc.getEngine().getDelegatedTask()) != null) {
		                	task.run();
		                }
		                handshakeStatus = cc.getEngine().getHandshakeStatus();
				 } else if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
					 releasePipesForUse();
					 assert(-1 == coordinator.checkForResponsePipeLineIdx(cc.getId())) : "should have already been relased";
					 processWork = false;
				 }
			}
		}
			
		//the normal case is to do this however we do need to skip for TLS wrap
		if (processWork) {
			
				int responsePipeLineIdx = cc.getPoolReservation();
				
				final boolean newBeginning = (responsePipeLineIdx<0);
						
				if (newBeginning) {
	
					//this release is required in case we are swapping pipe lines, we ensure that the latest sequence no is stored.
					releasePipesForUse();
					responsePipeLineIdx = coordinator.responsePipeLineIdx(channelId);
					
					if (-1 == responsePipeLineIdx) { 
						//logger.info("second check for relased pipe");
						releasePipesForUse();
						responsePipeLineIdx = coordinator.responsePipeLineIdx(channelId);
					}
					if (responsePipeLineIdx >= 0) {
						cc.setPoolReservation(responsePipeLineIdx);
					}
					
					//logger.info("new beginning {}",responsePipeLineIdx);
				
					
				} else {
					//logger.info("use existing return with {} is valid {} ",responsePipeLineIdx, cc.isValid);
				}
					
				if (responsePipeLineIdx >= 0) {
					
					int pumpState = pumpByteChannelIntoPipe(socketChannel, channelId, output[responsePipeLineIdx], newBeginning, cc, selection); 
		            					
					if (pumpState > 0) { 
		            	//logger.info("remove this selection");
		            	assert(1==pumpState) : "Can only remove if all the data is known to be consumed";
		            	removeSelection(selection);
		            } else {		            	
		            	//logger.info("can not remove this selection for channelId {} pump state {}",channelId,pumpState);
		            }
		           // logger.info("pushed data out");
		            if ((++rMask&0x3F) == 0) {
		             releasePipesForUse(); //must run but not on every pass
		            }
		           // logger.info("finished pipe release");
				}
		        
		}
	}

	private void removeSelection(SelectionKey selection) {
		
//		System.err.println("removed "+((ConnectionContext)selection.attachment()).getChannelId());
		
		doneSelectors.add(selection);//add to list for removal
		pendingSelections--;
		
//		if (pendingSelections==0) {
////			System.err.println("we now have zero selectors *************");
////			System.err.println("remove all the stored keys "+doneSelectors.size());
//			removeDoneKeys(selector.selectedKeys());
//			doneSelectors.clear();
//		}
		
	}

	private int rMask = 0;
	
	private void releasePipesForUse() {
		int i = releasePipes.length;
		while (--i>=0) {
			Pipe<ReleaseSchema> a = releasePipes[i];
			//logger.info("release pipes from {}",a);
			while (Pipe.hasContentToRead(a)) {
	    						
	    		int msgIdx = Pipe.takeMsgIdx(a);
	    		
	    		if (msgIdx == ReleaseSchema.MSG_RELEASEWITHSEQ_101) {
	    			
	    			long connectionId = Pipe.takeLong(a);
	    			
	    			//logger.info("release with sequence id {}",connectionId);
	    				    			
	    			
	    			long pos = Pipe.takeLong(a);	    			
	    			int seq = Pipe.takeInt(a);	    				    			
	    			releaseIfUnused(msgIdx, connectionId, pos, seq);
	    			Pipe.confirmLowLevelRead(a, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASEWITHSEQ_101));
	    				    			
	    			//remove any selection keys associated with this connection...	    			
	    			connectionIdToRemove = connectionId;
	    			selector.selectedKeys().forEach(selectionKeyRemoval);
	    			
	    			
	    		} else if (msgIdx == ReleaseSchema.MSG_RELEASE_100) {
	    			
	    			logger.info("warning, legacy (client side) release use detected in the server.");
	    			
	    			long connectionId = Pipe.takeLong(a);
	    			
	    			long pos = Pipe.takeLong(a);	    					
	    			releaseIfUnused(msgIdx, connectionId, pos, -1);
	    			Pipe.confirmLowLevelRead(a, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASE_100));
	    			
	    			connectionIdToRemove = connectionId;
	    			selector.selectedKeys().forEach(selectionKeyRemoval);
	    			
	    		} else {
	    			logger.info("unknown or shutdown on release");
	    			assert(-1==msgIdx);
	    			shutdownInProgress = true;
	    			Pipe.confirmLowLevelRead(a, Pipe.EOF_SIZE);
	    		}
	    		Pipe.releaseReadLock(a);	    		
	    	}
		}
	}

	//TODO: these release ofen before opening a new oen must check priorty for this connection...		
	private void releaseIfUnused(int id, long idToClear, long pos, int seq) {
		if (idToClear<0) {
			throw new UnsupportedOperationException();
		}
		
		
		int pipeIdx = coordinator.checkForResponsePipeLineIdx(idToClear);
		//if we can not look it  up then we can not release it?
		
		
		///////////////////////////////////////////////////
		//if sent tail matches the current head then this pipe has nothing in flight and can be re-assigned
		if (pipeIdx>=0 && (Pipe.headPosition(output[pipeIdx]) == pos)) {
		//	logger.info("NEW RELEASE for pipe {} connection {}",pipeIdx, idToClear);
			coordinator.releaseResponsePipeLineIdx(idToClear);
			
			assert( 0 == Pipe.releasePendingByteCount(output[pipeIdx]));
						
			if (id == ReleaseSchema.MSG_RELEASEWITHSEQ_101) {				
				SSLConnection conn = coordinator.connectionForSessionId(idToClear);
				if (null!=conn) {					
					conn.setSequenceNo(seq);//only set when we release a pipe
				}
				conn.clearPoolReservation();
				
			} 
		}
	}

    private boolean hasNewDataToRead() {
    	
    	if (pendingSelections>0) {
    		return true;
    	}
    	
  //no longer true remove,,,  	assert (0 == selector.selectedKeys().size());
    	    	
        try {        	        	
        	/////////////
        	//CAUTION - select now clears pevious count and only returns the additional I/O opeation counts which have become avail since the last time SelectNow was called
        	////////////        	
            pendingSelections = selector.selectNow();

        //    logger.info("pending new selections {} ",pendingSelections);
            return pendingSelections > 0;
        } catch (IOException e) {
            logger.error("unexpected shutdown, Selector for this group of connections has crashed with ",e);
            shutdownInProgress = true;
            return false;
        }
    }
    
    
    
    //returns -1 for did not start, 0 for started, and 1 for finished all.
    public int pumpByteChannelIntoPipe(SocketChannel sourceChannel, long channelId, Pipe<NetPayloadSchema> targetPipe, boolean newBeginning, SSLConnection cc, SelectionKey selection) {
    	
        //keep appending messages until the channel is empty or the pipe is full
    	long len = 0;//if data is read then we build a record around it
    	ByteBuffer[] b = null;
    	long temp = 0;
        if (Pipe.hasRoomForWrite(targetPipe)) {          
        	//logger.info("pump block for {} ",channelId);
            try {                
                
                //NOTE: the byte buffer is no longer than the valid maximum length but may be shorter based on end of wrap arround
                b = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(targetPipe), targetPipe);
                       
                int r1 = b[0].remaining();
                int r2 = b[1].remaining();
                
                temp = sourceChannel.read(b);
                
            	
            	if (temp>0){
            		len+=temp;
            	}            
                 
                int readCount = (r1-b[0].remaining())+(r2-b[1].remaining());
                assert(readCount == len) : "server "+readCount+" vs "+len;
                
                if (temp<0) {
                	//logger.info("client disconnected, so release");
                	//client was disconnected so release all our resources to ensure they can be used by new connections.
                	selection.cancel();                	
                	coordinator.releaseResponsePipeLineIdx(cc.id);    
    				cc.clearPoolReservation();

                }
                return publishOrAbandon(channelId, targetPipe, len, b, temp>=0, newBeginning, cc);

            } catch (IOException e) {
            	
            	    logger.trace("client closed connection ",e.getMessage());
            	
	             	selection.cancel();                	
	            	coordinator.releaseResponsePipeLineIdx(cc.id);    
					cc.clearPoolReservation();
            	
					return publishOrAbandon(channelId, targetPipe, len, b, temp>=0, newBeginning, cc);
            }
        } else {
        	//logger.info("try again later, unable to launch do to lack of room in {} ",targetPipe);
        	return -1;
        }
    }

	private int publishOrAbandon(long channelId, Pipe<NetPayloadSchema> targetPipe, long len, ByteBuffer[] b, boolean isOpen, boolean newBeginning, SSLConnection cc) {
		//logger.info("{} publish or abandon",System.currentTimeMillis());
		if (len>0) {
			
			if (newBeginning) {	
					
				Pipe.presumeRoomForWrite(targetPipe);
				
				int size = Pipe.addMsgIdx(targetPipe, NetPayloadSchema.MSG_BEGIN_208);
				Pipe.addIntValue(cc.getSequenceNo(), targetPipe);						
				Pipe.confirmLowLevelWrite(targetPipe, size);
				Pipe.publishWrites(targetPipe);
				
			}				
			
		//	logger.info("normal publish for connection {}     {}     {} channelID {} ",  cc.id, targetPipe, messageType, channelId );
			assert(cc.id == channelId) : "should match "+cc.id+" vs "+channelId;
			
	
			boolean fullTarget = b[0].remaining()==0 && b[1].remaining()==0;   
	
			
			
//			bytesConsumed+=len;
			publishData(targetPipe, channelId, len, cc);                  	 
			//logger.info("{} wrote {} bytess to pipe {} return code: {}", System.currentTimeMillis(), len,targetPipe,((fullTarget&&isOpen) ? 0 : 1));
			
			
			
			return (fullTarget && isOpen) ? 0 : 1; //only for 1 can we be sure we read all the data
			
			
		} else {
			 
			//logger.info("{} abandon one record, did not publish because length was {}    {}",System.currentTimeMillis(), len,targetPipe);

			 Pipe.unstoreBlobWorkingHeadPosition(targetPipe);//we did not use or need the writing buffers above.
			 
             if (isOpen && newBeginning) { //Gatling does this a lot, TODO: we should optimize this case.
             	//we will abandon but we also must release the reservation because it was never used
             	coordinator.releaseResponsePipeLineIdx(channelId);
             	SSLConnection conn = coordinator.connectionForSessionId(channelId);
             	conn.clearPoolReservation();
             //	logger.info("client is sending zero bytes, ZERO LENGTH RELESE OF UNUSED PIPE  FOR {}", channelId);
             }
             
             if (!isOpen) {
            	 cc.close();
             }
			 
			 return 1;//yes we are done
		}
	}

    private void recordErrorAndClose(ReadableByteChannel sourceChannel, long ccId, IOException e) {
       //   logger.error("unable to read",e);
          //may have been closed while reading so stop
          if (null!=sourceChannel) {
              try {            	  
            	  coordinator.releaseResponsePipeLineIdx(ccId);
                  sourceChannel.close();
               } catch (IOException e1) {
                   logger.warn("unable to close channel",e1);
               }
              
          }
    }

    private void publishData(Pipe<NetPayloadSchema> targetPipe, long channelId, long len, SSLConnection cc) {

    	assert(len<Integer.MAX_VALUE) : "Error: blocks larger than 2GB are not yet supported";
        
        int size = Pipe.addMsgIdx(targetPipe, messageType);               
        Pipe.addLongValue(channelId, targetPipe);  
        Pipe.addLongValue(System.currentTimeMillis(), targetPipe);
        
        if (NetPayloadSchema.MSG_PLAIN_210 == messageType) {
        	Pipe.addLongValue(-1, targetPipe);
        }
        
        int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(targetPipe);

        
        
        if (ServerCoordinator.TEST_RECORDS) {
           testValidContent(cc.getPoolReservation(), targetPipe, originalBlobPosition, (int)len);
        }
        
//ONLY VALID FOR UTF8

        if (showRequests) {
        	logger.info("//////////////////Server read for channel {} bPos{} len {} \n{}\n/////////////////////",channelId, originalBlobPosition, len, 
        			Appendables.appendUTF8(new StringBuilder(), targetPipe.blobRing, originalBlobPosition, (int)len, targetPipe.blobMask));               
        }
        
        
        Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int)len, targetPipe);  
        
        //all breaks are detected by the router not here
        //(section 4.1 of RFC 2616) end of header is \r\n\r\n but some may only send \n\n
        //
   
        Pipe.confirmLowLevelWrite(targetPipe, size);
        Pipe.publishWrites(targetPipe);
        //logger.info("done with publish pipe is now "+targetPipe);
    }
    
    
	private void testValidContent(final int idx, Pipe<NetPayloadSchema> pipe, int pos, int len) {
		
		if (ServerCoordinator.TEST_RECORDS) {
			
			//write pipeIdx identifier.
			//Appendables.appendUTF8(System.out, target.blobRing, originalBlobPosition, readCount, target.blobMask);
		
			
			boolean confirmExpectedRequests = true;
			if (confirmExpectedRequests) {
				Appendables.appendUTF8(accumulators[idx], pipe.blobRing, pos, len, pipe.blobMask);						    				
				
				while (accumulators[idx].length() >= HTTPUtil.expectedGet.length()) {
					
				   int c = startsWith(accumulators[idx],HTTPUtil.expectedGet); 
				   if (c>0) {					   
					   String remaining = accumulators[idx].substring(c*HTTPUtil.expectedGet.length());
					   accumulators[idx].setLength(0);
					   accumulators[idx].append(remaining);
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
    
    
}
