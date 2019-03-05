package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.PoolIdx;
import com.ociweb.pronghorn.util.PoolIdxPredicate;
import com.ociweb.pronghorn.util.SelectedKeyHashMapHolder;

/**
 * Server-side stage that reads from the socket. Useful for building a server.
 * Accepts unexpected calls (unlike ClientSocketReaderStage).
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ServerSocketReaderStage extends PronghornStage {
   
	private final int messageType;
	private final int singleMessageSpace;
	private final int reqPumpPipeSpace;

	public static final Logger logger = LoggerFactory.getLogger(ServerSocketReaderStage.class);
    
    private final Pipe<NetPayloadSchema>[] output;
    private final Pipe<ReleaseSchema>[] releasePipes;
    private final ServerCoordinator coordinator;

    private final Selector selector;
    
    private final GraphManager gm;
    
    public static boolean showRequests = false;
    
    private boolean shutdownInProgress;
    private final String label;

    private Set<SelectionKey> selectedKeys;	
    private ArrayList<SelectionKey> doneSelectors = new ArrayList<SelectionKey>(100);

	private PoolIdx responsePipeLinePool;
	
    private final class PipeLineFilter implements PoolIdxPredicate {
		
    	public final int groups;
    	private int groupSize;
    	private int okGroup;

		private PipeLineFilter(int groups, int groupSize) {
		     this.groups = groups;
		     this.groupSize = groupSize;
		}

		public void setId(long ccId) {
			okGroup = (int)(ccId%groups);			
		}

		@Override
		public boolean isOk(final int i) {
			return okGroup == (i/groupSize);
		}
	}
    
    public static ServerSocketReaderStage newInstance(GraphManager graphManager, Pipe<ReleaseSchema>[] ack, Pipe<NetPayloadSchema>[] output, 
    		                                          ServerCoordinator coordinator) {
        return new ServerSocketReaderStage(graphManager, ack, output, coordinator);
    }

	/**
	 *
	 * @param graphManager
	 * @param ack _in_ The release acknowledgment.
	 * @param output _out_ The read payload from the socket.
	 * @param coordinator
	 */
	public ServerSocketReaderStage(GraphManager graphManager, Pipe<ReleaseSchema>[] ack, Pipe<NetPayloadSchema>[] output,
			                       ServerCoordinator coordinator) {
        super(graphManager, ack, output);
        this.coordinator = coordinator;
       
        this.label = "\n"+coordinator.host()+":"+coordinator.port()+"\n";
        
        this.output = output;
        this.releasePipes = ack;

        this.messageType = coordinator.isTLS ? NetPayloadSchema.MSG_ENCRYPTED_200 : NetPayloadSchema.MSG_PLAIN_210;
        coordinator.setStart(this);
        
        GraphManager.addNota(graphManager, GraphManager.PRODUCER, GraphManager.PRODUCER, this);
        GraphManager.addNota(graphManager, GraphManager.LOAD_BALANCER, GraphManager.LOAD_BALANCER, this);
        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);

        Number dsr = graphManager.defaultScheduleRate();
        if (dsr!=null) {
        	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, dsr.longValue()/2, this);
        }
               
		//        //If server socket reader does not catch the data it may be lost
		//        GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
      
        //if too slow we may loose data but if too fast we may starve others needing data
        //GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, 100_000L, this);
      
        
        //if we only have 1 router then do not isolate the reader.
        if (ack.length>1) {
        	//can spin to pick up all the work and may starve others
        	GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
        }
        //much larger limit since nothing needs this thread back.
        GraphManager.addNota(graphManager, GraphManager.SLA_LATENCY, 100_000_000, this);
        
        singleMessageSpace = Pipe.sizeOf(NetPayloadSchema.instance, messageType);
        reqPumpPipeSpace = singleMessageSpace + Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_BEGIN_208);
        
        gm = graphManager;
        
        try {//must be done early to ensure this is ready before the other stages startup.
        	coordinator.registerSelector(selector = Selector.open());
        } catch (IOException e) {
        	throw new RuntimeException(e);
        }

    }
        
    @Override
    public String toString() {
    	return super.toString()+label;
    }

    @Override
    public void startup() {
    	
    	//find first pipes all going to the same place
    	//we then assume we have groups of this size, this can easily be asserted
    	int sizeOfOneGroup = output.length;
    	int lastId = -1;
    	int i = output.length;
    	while (--i>=0) {
    		
    		int consumer = GraphManager.getRingConsumerStageId(gm, output[i].id);
    		if (lastId!=-1 && lastId!=consumer) {;
    			sizeOfOneGroup = (output.length-1)-i;
    			break;
    		}    		
    		lastId = consumer;
    	}
    	///////////////////////////////////
    	int groups = output.length/sizeOfOneGroup;
    	if (groups*sizeOfOneGroup != output.length) {
    		//not even so just use 1
    		groups = 1;
    	}
    	     
    	this.responsePipeLinePool = new PoolIdx(output.length, groups); 	
        	
    	this.selectedKeyHolder = new SelectedKeyHashMapHolder();
		
        ServerCoordinator.newSocketChannelHolder(coordinator);
                
      
    }
    
    @Override
    public void shutdown() {
    	Pipe.publishEOF(output);  
       
        logger.trace("server reader has shut down");
    }
    
    boolean hasRoomForMore = true;
    public final Consumer<SelectionKey> selectionKeyAction = new Consumer<SelectionKey>(){
			@Override
			public void accept(SelectionKey selection) {
				hasRoomForMore &= processSelection(selection); 
				
				doneSelectors.add(selection);//remove them all..
			}
    };    

    private SelectedKeyHashMapHolder selectedKeyHolder;
	private final BiConsumer keyVisitor = new BiConsumer() {
		@Override
		public void accept(Object k, Object v) {
			selectionKeyAction.accept((SelectionKey)k);
		}
	};

	//final int waitForPipeConsume = 10;

    @Override
    public void run() {
 
    	 if(!shutdownInProgress) {
 	        	releasePipesForUse();//clear now so select will pick up more

    	   		///////////////////
    	   		//after this point we are always checking for new data work so always record this
    	   		////////////////////
    	    	if (null != this.didWorkMonitor) {
    	    		this.didWorkMonitor.published();
    	    	}
    	   		
    	   
    	        ////////////////////////////////////////
    	        ///Read from socket
    	        ////////////////////////////////////////
    	    	int iter = 1;//if no data go around one more time..
    	    	do {
	    	        if (hasNewDataToRead(selector)) { 
	    	        	iter = 1;
	        	    		        	   		
	    	            doneSelectors.clear();
	    	            hasRoomForMore = true; //set this up before we visit
	    	            
	    	            HashMap<SelectionKey, ?> keyMap = selectedKeyHolder.selectedKeyMap(selectedKeys);
	    	            if (null!=keyMap) {
	    	               keyMap.forEach(keyVisitor);
	    	            } else {
	    	         	   //fall back to old if the map can not be found.
	    	         	   selectedKeys.forEach(selectionKeyAction);
	    	            }

	    	    	     
	    	            //TODO: this has reintroduced the overloading hang bug because we have no timer
	    	            //      the change did not help that much..
	    	            
	    	            //TODO: restore old loop, only remove when we have consumed the data
	    	            //      this will reduce the selection calls since nothing will repeat
	    	            //      old data does need a timeout however to ensure we do not hang!!!
	    	            
	    	            
	    	            removeDoneKeys(selectedKeys);
	    	            
	    	            if (!hasRoomForMore) {
	    	            	return;
	    	            } 
	    	        }
	    	        releasePipesForUse();//clear now so select will pick up more
    	    	} while (--iter>=0);
    	   
    	    	
    	 
    	 } else {
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
        
    }
  


	private void removeDoneKeys(Set<SelectionKey> selectedKeys) {
		//sad but this is the best way to remove these without allocating a new iterator
		// the selectedKeys.removeAll(doneSelectors); will produce garbage upon every call
		ArrayList<SelectionKey> doneSelectors2 = doneSelectors;
		int c = doneSelectors2.size();
		while (--c>=0) {
		    	selectedKeys.remove(doneSelectors2.get(c));
		}

	}

	private boolean processSelection(SelectionKey selection) {
		
		assert isRead(selection) : "only expected read"; 
		//get the context object so we know what the channel identifier is
		ConnectionContext connectionContext = (ConnectionContext)selection.attachment();                
		long channelId = connectionContext.getChannelId();
		assert(channelId>=0);

		//logger.info("\nnew key selection in reader for connection {}",channelId);
		
		return processConnection(selection, channelId, coordinator.lookupConnectionById(channelId));

	}
	
	private boolean processConnection(SelectionKey selection, long channelId,
			BaseConnection cc) {
		if (null != cc) {
			if (!coordinator.isTLS) {	
			} else {
				handshakeTaskOrWrap(cc);
			}		
			
			return processConnection2(selection, channelId, cc, cc.getPoolReservation());
		} else {
			return processClosedConnection(selection, channelId);
		}
	}

	private boolean processConnection2(SelectionKey selection, long channelId,
										BaseConnection cc, int responsePipeLineIdx) {
		
		final boolean newBeginning = responsePipeLineIdx<0;
		
		if (!newBeginning) {
			return (pumpByteChannelIntoPipe(cc.getSocketChannel(), cc.id, 
					cc.getSequenceNo(), output[responsePipeLineIdx], 
                    newBeginning, cc, selection)==1);
		} else {
			return processConnectionBegin(selection, cc, newBeginning, beginningProcessing(channelId, cc));
		}
	}

	private boolean processConnectionBegin(SelectionKey selection, BaseConnection cc, final boolean newBeginning,
			int responsePipeLineIdx) {
		if (responsePipeLineIdx >= 0) {		
			return (pumpByteChannelIntoPipe(cc.getSocketChannel(), cc.id, 
					cc.getSequenceNo(), output[responsePipeLineIdx], 
					newBeginning, cc, selection)==1);
		} else {			
			return true;
		}
	}

	private int beginningProcessing(long channelId, BaseConnection cc) {
		int responsePipeLineIdx;
		//this connection lost the lock on this outgoing pipe so we need to look it up.				
		responsePipeLineIdx = lookupResponsePipeLineIdx(channelId, cc);
		if (responsePipeLineIdx>=0 && (Pipe.hasRoomForWrite(output[responsePipeLineIdx], reqPumpPipeSpace)) ) {
			//we got the pipe lock so keep it until we finish a "block"
			cc.setPoolReservation(responsePipeLineIdx);
		} else {
			//logger.info("\n too much load");
		}
		
		//debugPipeAssignment(channelId, responsePipeLineIdx);
		/////////////////////////////////////
		return responsePipeLineIdx;
	}


	private int lookupResponsePipeLineIdx(long channelId, BaseConnection cc) {
		if (cc.id == channelId) {			
			int idx = cc.getPreviousPoolReservation();
			
			if (-1!=idx) {
				idx = responsePipeLinePool.optimisticGet(channelId, idx);
				if (-1!=idx) {
					return idx;
				}
			} 			
			
			//key & group to ensure connection is sent to same track
			//linear search because the previous in cc has been replaced since we are sharing this position
			return responsePipeLinePool.get(channelId, (int)channelId%responsePipeLinePool.groups);	
			
		} else {
			
			releaseOldChannelId(channelId);
			//this release is required in case we are swapping pipe lines, we ensure that the latest sequence no is stored.
			releasePipesForUse();
		
			//key & group to ensure connection is sent to same track	
			return responsePipeLinePool.get(channelId, (int)channelId%responsePipeLinePool.groups);	
		}
	}

	private void releaseOldChannelId(long channelId) {
		//we must release the old one?
		int result = PoolIdx.getIfReserved(responsePipeLinePool,channelId);
		if (result>=0) {
			//we found the old channel so remove it before we add the new id.
		    responsePipeLinePool.release(channelId);
		}
	}

//	private void debugPipeAssignment(long channelId, int responsePipeLineIdx) {
//		final boolean debugPipeAssignment = false;
//		if (debugPipeAssignment) {
//			if (!"Telemetry Server".equals(coordinator.serviceName())) {//do not show for monitor
//
//				logger.info("\nstage: {} begin channel id {} pipe line idx {} out of {} ",
//						this.stageId,
//						channelId, 
//						responsePipeLineIdx,
//						output.length);
//			}
//		}
//	}

	private void handshakeTaskOrWrap(BaseConnection cc) {
		if (null!=cc && null!=cc.getEngine()) {
			 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();
			 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
		            Runnable task;//TODO: there is an opportunity to have this done by a different stage in the future.
		            while ((task = cc.getEngine().getDelegatedTask()) != null) {
		            	task.run();
		            }
			 } else if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
				 releasePipesForUse();

			 }				 
		}
	}
	

	private boolean processClosedConnection(SelectionKey selection, long channelId) {
		//logger.info("closed connection here for channel {}",channelId);
		final SocketChannel socketChannel = (SocketChannel) selection.channel();
		assert(validateClose(socketChannel, channelId));
		try {
			socketChannel.close();
		} catch (IOException e) {				
		}
		//if this selection was closed then remove it from the selections

		if (PoolIdx.getIfReserved(responsePipeLinePool,channelId)>=0) {
			responsePipeLinePool.release(channelId);
		}			

		//doneSelectors.add(selection);//remove them all.
		return true;
	} 

		
	////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////

	private boolean validateClose(final SocketChannel socketChannel, final long channelId) {

		try {
			int len = socketChannel.read(ByteBuffer.allocate(3));
			if (len>0) {
				logger.trace("client connection is sending addional data after closing connection: {}",socketChannel);
			}
		} catch (IOException e) {
			//ignore
		}		
		return true;
	}

	private boolean isRead(SelectionKey selection) {
		try {
			return 0 != (SelectionKey.OP_READ & selection.readyOps());
		} catch (Exception e) {
			return true;//this is not relevant to the check.
		}
	}

	
	private void releasePipesForUse() {
		int i = releasePipes.length;
		while (--i>=0) {
			Pipe<ReleaseSchema> a = releasePipes[i];
			//logger.info("{}: release pipes from {}",i,a);
			while ((!Pipe.isEmpty(a)) && Pipe.hasContentToRead(a)) {	    						
	    		consumeReleaseMessage(a);	    		
	    	}
		}
	}

	private void consumeReleaseMessage(Pipe<ReleaseSchema> a) {
		final int msgIdx = Pipe.takeMsgIdx(a);
		
		if (msgIdx == ReleaseSchema.MSG_RELEASEWITHSEQ_101) {
			
			releaseIfUnused(msgIdx, Pipe.takeLong(a), Pipe.takeLong(a), Pipe.takeInt(a), Pipe.takeInt(a));
			Pipe.confirmLowLevelRead(a, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASEWITHSEQ_101));

		} else if (msgIdx == ReleaseSchema.MSG_RELEASE_100) {
			
			releaseIfUnused(msgIdx, Pipe.takeLong(a), Pipe.takeLong(a), -1, -1);
			Pipe.confirmLowLevelRead(a, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASE_100));
  
		} else {
			endOfReleaseMessages(a, msgIdx);
		}
		
		Pipe.releaseReadLock(a);
	}

	private void endOfReleaseMessages(Pipe<ReleaseSchema> a, int msgIdx) {
		logger.trace("unknown or shutdown on release");
		assert(-1==msgIdx) : "unspected msgId "+msgIdx;
		shutdownInProgress = true;
		Pipe.confirmLowLevelRead(a, Pipe.EOF_SIZE);
	}
		
	private void releaseIfUnused(int id, long idToClear, long pos, int seq, int providedPipeIdx) {
		assert(idToClear>=0);
				
		BaseConnection cc = coordinator.lookupConnectionById(idToClear);
		//only release if we have a connection to be released.
		if (null!=cc) {
			int pipeIdx;
			if (providedPipeIdx>=0) {
				pipeIdx = providedPipeIdx;
				assert ((-1== lookupPipeId(idToClear, cc)) 
						|| (lookupPipeId(idToClear, cc) == providedPipeIdx)) : "internall error";
				
			} else {			
				pipeIdx = lookupPipeId(idToClear, cc);
			}
					
			///////////////////////////////////////////////////
			//if sent tail matches the current head then this pipe has nothing in flight and can be re-assigned
			if (pipeIdx>=0 && Pipe.workingHeadPosition(output[pipeIdx])<=pos && (Pipe.headPosition(output[pipeIdx]) == pos)) {
	       	    releaseIdx(id, idToClear, seq, cc, pipeIdx); 		
			}
		} else {
			int pipeIdx = (providedPipeIdx>=0) ? providedPipeIdx : PoolIdx.getIfReserved(responsePipeLinePool, idToClear);
			if (pipeIdx!=-1) {
				PoolIdx.release(responsePipeLinePool, idToClear, pipeIdx);
			}
			
		}
		
		
	}

	private void releaseIdx(int id, long idToClear, int seq, BaseConnection cc, int pipeIdx) {
		PoolIdx.release(responsePipeLinePool, idToClear, pipeIdx);
		
		assert( 0 == Pipe.releasePendingByteCount(output[pipeIdx]));
					
		if (id == ReleaseSchema.MSG_RELEASEWITHSEQ_101) {				
			if (null!=cc) {					
				cc.setSequenceNo(seq);//only set when we release a pipe					
				cc.clearPoolReservation();
			}				
		}
	}

	private int lookupPipeId(long idToClear, BaseConnection cc) {
		int pipeIdx = null!=cc? cc.getPoolReservation() : -1;
		if (pipeIdx < 0) {
			pipeIdx = null!=cc? cc.getPreviousPoolReservation() : -1;
			if (pipeIdx < 0) {
				pipeIdx = PoolIdx.getIfReserved(responsePipeLinePool, idToClear);
			}
		}
		return pipeIdx;
	}

	private boolean hasNewDataToRead(Selector selector) {
    	
    	//assert(null==selectedKeys || selectedKeys.isEmpty()) : "All selections should be processed";
//    	if (null!=selectedKeys && !selectedKeys.isEmpty()) {
//    		return true; //keep this!
//    	}
    	
        try {
                	
        	////////////
        	//CAUTION - select now clears pevious count and only returns the additional I/O opeation counts which have become avail since the last time SelectNow was called
        	////////////        
        	if (selector.selectNow() > 0) {            	
            	selectedKeys = selector.selectedKeys();
            	
            	//we often select ALL the sent fields so what is going wrong??
            	//System.out.println(selectedKeys.size()+" selection block "+System.currentTimeMillis()); 
            	
            	return true;
            } else {
            	return false;
            }

            //    logger.info("pending new selections {} ",pendingSelections);
        } catch (IOException e) {
            logger.error("unexpected shutdown, Selector for this group of connections has crashed with ",e);
            shutdownInProgress = true;
            return false;
        }
    }

    private int r1; //needed by assert
    private int r2; //needed by assert
    
    //returns -1 for did not start, 0 for started, and 1 for finished all.
    private int pumpByteChannelIntoPipe(SocketChannel sourceChannel, 
    		long channelId, int sequenceNo, Pipe<NetPayloadSchema> targetPipe, 
    		boolean newBeginning, BaseConnection cc, SelectionKey selection) {

    	//keep appending messages until the channel is empty or the pipe is full
    	long len = 0;//if data is read then we build a record around it
    	ByteBuffer[] targetBuffer = null;
    	long temp = 0;

		if (Pipe.hasRoomForWrite(targetPipe, reqPumpPipeSpace)) {
			//only call nano if we are consuming this one.
			cc.setLastUsedTime(System.nanoTime());//needed to know when this connection can be disposed
			
            try {                
                
            	//Read as much data as we can...
            	//We must make the writing buffers larger based on how many messages we can support
            	int readMaxSize = targetPipe.maxVarLen;           	
            	long units = targetPipe.sizeOfSlabRing - (Pipe.headPosition(targetPipe)-Pipe.tailPosition(targetPipe));
            	units -= reqPumpPipeSpace;
            	if (units>0) {
	            	int extras = (int)units/singleMessageSpace;
	            	if (extras>0) {
	            		readMaxSize += (extras*targetPipe.maxVarLen);
	            	}
            	}
            	
                //NOTE: the byte buffer is no longer than the valid maximum length but may be shorter based on end of wrap around
				int wrkHeadPos = Pipe.storeBlobWorkingHeadPosition(targetPipe);
				targetBuffer = Pipe.wrappedWritingBuffers(wrkHeadPos, 
                		                       targetPipe, 
                		                       readMaxSize);
                       
                assert(collectRemainingCount(targetBuffer));
     
                
                //read as much as we can, one read is often not enough for high volume
                boolean isStreaming = false; //TODO: expose this switch..
                
//    			try {
//    				sourceChannel.setOption(ExtendedSocketOptions.TCP_QUICKACK, Boolean.TRUE);
//    			} catch (IOException e1) {
//    				// TODO Auto-generated catch block
//    				e1.printStackTrace();
//    			}	
    			
           //     synchronized(sourceChannel) {
	                do {
		                temp = sourceChannel.read(targetBuffer);
		            	if (temp>0){
		            		len+=temp;
		            	}
	                } while (temp>0 && isStreaming); //for multiple in flight pipelined must keep reading...
          //      }
                
                assert(readCountMatchesLength(len, targetBuffer));
                
//                if (temp<=0) {
//                	doneSelectors.add(selection);
//                }
                if (temp>=0 & cc!=null && cc.isValid() && !cc.isDisconnecting()) { 
                
                	
					if (len>0) {
						return publishData(channelId, sequenceNo, targetPipe, len, targetBuffer, true, newBeginning);
					} else {
						Pipe.unstoreBlobWorkingHeadPosition(targetPipe);
						return 1;
					}
                } else {
                	//logger.info("client disconnected, so release");
                
                	if (null!=cc) {
                		cc.clearPoolReservation();
                	}
					//client was disconnected so release all our resources to ensure they can be used by new connections.
					selection.cancel();                	
					responsePipeLinePool.release(channelId);
					//to abandon this must be negative.				
					int result = abandonConnection(channelId, targetPipe, false, newBeginning);
					
                	if (null!=cc) {
                		cc.close();
                	}
                	return result;
                }

            } catch (IOException e) {
            	
            	    logger.trace("client closed connection ",e.getMessage());
            	
					boolean isOpen = temp>=0;
					int result;
					if (len>0) {			
						result = publishData(channelId, cc.getSequenceNo(), targetPipe, len, targetBuffer, isOpen, newBeginning);
					} else {
						result = abandonConnection(channelId, targetPipe, isOpen, newBeginning);
					}            	
					selection.cancel();                	
	            	responsePipeLinePool.release(cc.id);    
					cc.clearPoolReservation();
					if (temp<0) {
						cc.close();
					}
					
					return result;
            }
        } else {
        	//logger.info("\ntry again later, unable to launch do to lack of room in {} ",targetPipe);
        	return -1;
        }
    }

	private boolean collectRemainingCount(ByteBuffer[] b) {
		r1 = b[0].remaining();
		r2 = b[1].remaining();
		return true;
	}

	private final boolean readCountMatchesLength(long len, ByteBuffer[] b) {
		int readCount = (r1-b[0].remaining())+(r2-b[1].remaining());
		assert(readCount == len) : "server "+readCount+" vs "+len;
		return true;
	}

	private int abandonConnection(long channelId, Pipe<NetPayloadSchema> targetPipe, boolean isOpen,
			boolean newBeginning) {
		//logger.info("{} abandon one record, did not publish because length was {}    {}",System.currentTimeMillis(), len,targetPipe);

		 Pipe.unstoreBlobWorkingHeadPosition(targetPipe);//we did not use or need the writing buffers above.
		 
		 if (isOpen && newBeginning) {
			 //Gatling does this a lot, TODO: we should optimize this case.
		 	//we will abandon but we also must release the reservation because it was never used
		 	responsePipeLinePool.release(channelId);
		 	BaseConnection conn = coordinator.lookupConnectionById(channelId);
		 	if (null!=conn) {
		 		conn.clearPoolReservation();
		 	}
		 //	logger.info("client is sending zero bytes, ZERO LENGTH RELESE OF UNUSED PIPE  FOR {}", channelId);
		 }

		 
		 return 1;//yes we are done
	}

	private int publishData(long channelId, int sequenceNo, Pipe<NetPayloadSchema> targetPipe, long len, ByteBuffer[] b,
			boolean isOpen, boolean newBeginning) {
		if (newBeginning) {	
										
			Pipe.presumeRoomForWrite(targetPipe);
			
			int size = Pipe.addMsgIdx(targetPipe, NetPayloadSchema.MSG_BEGIN_208);
			Pipe.addIntValue(sequenceNo, targetPipe);						
			Pipe.confirmLowLevelWrite(targetPipe, size);
			Pipe.batchFollowingPublishes(targetPipe, 1);
			
			Pipe.publishWrites(targetPipe); //wait on publish until the rest is ready...
			
		}				

		
		boolean fullTarget = b[0].remaining()==0 && b[1].remaining()==0;   

		Pipe.presumeRoomForWrite(targetPipe);
		
		if (messageType>=0) {
			
			publishSingleMessage(targetPipe, channelId, Pipe.unstoreBlobWorkingHeadPosition(targetPipe), len);     
			
			
		} else {
			Pipe.publishEOF(targetPipe);
			
//			final int size = Pipe.addMsgIdx(targetPipe, messageType);  
//			Pipe.confirmLowLevelWrite(targetPipe, size);
//		    Pipe.publishWrites(targetPipe);
		}
		
		
		//logger.info("{} wrote {} bytess to pipe {} return code: {}", System.currentTimeMillis(), len, targetPipe, ((fullTarget&&isOpen) ? 0 : 1));
		
		return (fullTarget && isOpen) ? 0 : 1; //only for 1 can we be sure we read all the data
	}
	
    private void publishSingleMessage(Pipe<NetPayloadSchema> targetPipe, 
		    		                 long channelId, 
		    		                 int pos, long remainLen) {
    	
    	while (remainLen>0 && Pipe.hasRoomForWrite(targetPipe)) {
    		
    		    int localLen = (int)Math.min(targetPipe.maxVarLen, remainLen);    		
	            final int size = Pipe.addMsgIdx(targetPipe, messageType);               
	
		        Pipe.addLongValue(channelId, targetPipe);  
		        Pipe.addLongValue(System.nanoTime(), targetPipe);
				
		        if (NetPayloadSchema.MSG_PLAIN_210 == messageType) {
		        	Pipe.addLongValue(-1, targetPipe);
		        }
		        		
		        if (showRequests) {	        	
		        	showRequests(targetPipe, channelId, pos, localLen);
		        }
		        		        
		        Pipe.moveBlobPointerAndRecordPosAndLength(pos, (int)localLen, targetPipe);  
		        
		        //all breaks are detected by the router not here
		        //(section 4.1 of RFC 2616) end of header is \r\n\r\n but some may only send \n\n
		        //
	  
	        Pipe.confirmLowLevelWrite(targetPipe, size);
	        Pipe.publishWrites(targetPipe);
	        Pipe.publishAllBatchedWrites(targetPipe); //Ensure we have nothing waiting, key for watchers.
	        
	        remainLen -= localLen;
	        pos += localLen;	        
    	}
    
    }

	private void showRequests(Pipe<NetPayloadSchema> targetPipe, long channelId, int pos, int localLen) {
		if (!"Telemetry Server".equals(coordinator.serviceName())) {
			try {
		    	//ONLY VALID FOR UTF8
		    	logger.info("/////////////\n/////Server read for channel {} bPos{} len {} \n{}\n/////////////////////",
		    			channelId, pos, localLen, 
		    			
		    			//TODO: the len here is wrong and must be  both the header size plus the payload size....
		    			
		    			Appendables.appendUTF8(new StringBuilder(), 
		    					targetPipe.blobRing, 
		    					pos, 
		    					(int)localLen, targetPipe.blobMask));             
			} catch (Exception e) {
				//ignore we are debugging.
			}
		}
	}

    
}
