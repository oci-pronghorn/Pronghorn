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
//import com.ociweb.pronghorn.util.SelectedKeyHashMapHolder;

/**
 * Server-side stage that reads from the socket. Useful for building a server.
 * Accepts unexpected calls (unlike ClientSocketReaderStage).
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ServerSocketReaderStage extends PronghornStage {
   

	private PipeLineFilter isOk;

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
		
    	private int groups;
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

//        //If server socket reader does not catch the data it may be lost
//        GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
        
        //if too slow we may loose data but if too fast we may starve others needing data
       // GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, 100_000L, this);
        //TODO: can we do this dynamic by checking pipe depths??
        
        
        
        
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
    	
    	this.isOk = new PipeLineFilter(groups, sizeOfOneGroup);      
    	this.responsePipeLinePool = new PoolIdx(output.length, groups); 	
        	
    	//this.selectedKeyHolder = new SelectedKeyHashMapHolder();
		
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

   // private SelectedKeyHashMapHolder selectedKeyHolder;
	private final BiConsumer keyVisitor = new BiConsumer() {
		@Override
		public void accept(Object k, Object v) {
			selectionKeyAction.accept((SelectionKey)k);
		}
	};


    @Override
    public void run() {
    	
    	 if(!shutdownInProgress) {

    	    	/////////////////////////////
    	    	//must keep this pipe from getting full or the processing will get backed up
    	    	////////////////////////////
    	   		releasePipesForUse();
      

    	   		///////////////////
    	   		//after this point we are always checking for new data work so always record this
    	   		////////////////////
    	    	if (null != this.didWorkMonitor) {
    	    		this.didWorkMonitor.published();
    	    	}
    	   		
    	        ////////////////////////////////////////
    	        ///Read from socket
    	        ////////////////////////////////////////

    	        if (hasNewDataToRead()) { //single & to ensure we check has new data to read.

    	           doneSelectors.clear();
    	           hasRoomForMore = true; //set this up before we visit

    	           //HashMap<SelectionKey, ?> keyMap = selectedKeyHolder.selectedKeyMap(selectedKeys);
    	           //if (null!=keyMap) {   
    				//   keyMap.forEach(keyVisitor);
    	           //} else {
    	        	   //fall back to old if the map can not be found.
    	        	   selectedKeys.forEach(selectionKeyAction);
    	           //}
    	           
    	           removeDoneKeys(selectedKeys);
    	           if (!hasRoomForMore) {
    	        	 return;//  break;
    	           } else {
    	        	   //do we need to detected has room for more better?
    	        	 //  break;
    	        	   
    	           }
    	        }
    	 
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

	//int[] selectedOuts = null;
	
	private boolean processSelection(SelectionKey selection) {
		
		assert isRead(selection) : "only expected read"; 
		final SocketChannel socketChannel = (SocketChannel)selection.channel();
		
		//get the context object so we know what the channel identifier is
		ConnectionContext connectionContext = (ConnectionContext)selection.attachment();                
		long channelId = connectionContext.getChannelId();
		assert(channelId>=0);

		//logger.info("\nnew key selection in reader for connection {}",channelId);
		
		BaseConnection cc = coordinator.lookupConnectionById(channelId);
		
		if (null != cc) {
			cc.setLastUsedTime(System.nanoTime());//needed to know when this connection can be disposed
		} else {
			//logger.info("closed connection here for channel {}",channelId);
			
			assert(validateClose(socketChannel, channelId));
			try {
				socketChannel.close();
			} catch (IOException e) {				
			}
			//if this selection was closed then remove it from the selections

			if (PoolIdx.getIfReserved(responsePipeLinePool,channelId)>=0) {
				responsePipeLinePool.release(channelId);
			}			

			return true;
		}

	//	boolean processWork = true;
		if (coordinator.isTLS) {
				
			if (null!=cc && null!=cc.getEngine()) {
				 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();
				 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
		                Runnable task;//TODO: there is an opportunity to have this done by a different stage in the future.
		                while ((task = cc.getEngine().getDelegatedTask()) != null) {
		                	task.run();
		                }
				 } else if (HandshakeStatus.NEED_WRAP == handshakeStatus) {
					 releasePipesForUse();
//					 processWork = false;
				 }				 
			}
		}
		
		
		boolean hasOutputRoom = true;
		//the normal case is to do this however we do need to skip for TLS wrap
		if (null!=cc) {
			
			  // ServerCoordinator.acceptConnectionRespond = System.nanoTime();
						
				int responsePipeLineIdx = cc.getPoolReservation();

				final boolean newBeginning = (responsePipeLineIdx<0);

				if (newBeginning) {
					
					if (cc.id != channelId) {
						//we must release the old one?
						int result = PoolIdx.getIfReserved(responsePipeLinePool,channelId);
						if (result>=0) {
							//we found the old channel so remove it before we add the new id.
							responsePipeLinePool.release(channelId);
						}
					}
					
					//this release is required in case we are swapping pipe lines, we ensure that the latest sequence no is stored.
					releasePipesForUse();
					isOk.setId(channelId); //key to ensure connection is sent to same track
					responsePipeLineIdx = responsePipeLinePool.get(channelId, isOk);
					
					if (-1 == responsePipeLineIdx) { 
						//logger.trace("second check for released pipe");
						Thread.yield();
						releasePipesForUse();
						isOk.setId(channelId); //key to ensure connection is sent to same track
						responsePipeLineIdx = responsePipeLinePool.get(channelId, isOk);
						if (-1 == responsePipeLineIdx) {
							//logger.info("\n too much load");
							//try later, we can not find an open pipe right now.
							return false;//must return false to ensure we leave this stage
						}
						
					}
					
					if (responsePipeLineIdx >= 0) {
						
						//debug to see if distribution is even.
//						if (null == selectedOuts) {
//							selectedOuts = new int[output.length];
//							
//						}
//						selectedOuts[responsePipeLineIdx]++;
//						
//						System.out.println(stageId+"  pipe selectios: "+Arrays.toString(selectedOuts));
						
						cc.setPoolReservation(responsePipeLineIdx);
					}
			
					
					final boolean debugPipeAssignment = false;
					if (debugPipeAssignment) {
						if (!"Telemetry Server".equals(coordinator.serviceName())) {//do not show for monitor
						
//							int i = output.length;
//							while (--i>=0) {
//																
//								//What about encryption unit does it hold data we have not yet processed??
//								System.out.println(this.stageId+"  "+i+" pos tail "+Pipe.tailPosition(output[i])+"  head "+Pipe.headPosition(output[i])+" len  "+Pipe.contentRemaining(output[i])+" total frags:"+Pipe.totalWrittenFragments(output[i]));
//							}					
											
							logger.info("\nstage: {} begin channel id {} pipe line idx {} out of {} ",
									this.stageId,
									channelId, 
									responsePipeLineIdx,
									output.length);
						}
					}
					/////////////////////////////////////
					
					
				} else {
		//			logger.info("use existing return with {} is valid {} connection {} ",responsePipeLineIdx, cc.isValid, cc.id);
				}
					
				if (responsePipeLineIdx >= 0) {
					int pumpState = pumpByteChannelIntoPipe(socketChannel, cc.id, 
															cc.getSequenceNo(),
							                                output[responsePipeLineIdx], 
							                                newBeginning, 
							                                cc, selection);
								
					if (pumpState<=0) {
						hasOutputRoom = false;
						//logger.info("\n no output room to read data");
					}					
				} 
		}
		return hasOutputRoom;
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
	    						
	    		int msgIdx = Pipe.takeMsgIdx(a);
	    		
	    		if (msgIdx == ReleaseSchema.MSG_RELEASEWITHSEQ_101) {
	    			
	    			
	    			long connectionId = Pipe.takeLong(a);
	    			
	    			//logger.info("release with sequence id {}",connectionId);
	    		
	    			long pos = Pipe.takeLong(a);	    			
	    			int seq = Pipe.takeInt(a);	    				    			
	    			releaseIfUnused(msgIdx, connectionId, pos, seq);
	    			Pipe.confirmLowLevelRead(a, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASEWITHSEQ_101));

	    		} else if (msgIdx == ReleaseSchema.MSG_RELEASE_100) {
	    			
	    			//logger.info("warning, legacy (client side) release use detected in the server.");
	    			
	    			long connectionId = Pipe.takeLong(a);
	    			
	    			long pos = Pipe.takeLong(a);	    					
	    			releaseIfUnused(msgIdx, connectionId, pos, -1);
	    			Pipe.confirmLowLevelRead(a, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASE_100));
	  
	    		} else {
	    			logger.info("unknown or shutdown on release");
	    			assert(-1==msgIdx) : "unspected msgId "+msgIdx;
	    			shutdownInProgress = true;
	    			Pipe.confirmLowLevelRead(a, Pipe.EOF_SIZE);
	    		}
	    		Pipe.releaseReadLock(a);	    		
	    	}
		}
	}
		
	private void releaseIfUnused(int id, long idToClear, long pos, int seq) {
		if (idToClear<0) {
			throw new UnsupportedOperationException();
		}
				
		int pipeIdx = PoolIdx.getIfReserved(responsePipeLinePool,idToClear);
		//if we can not look it  up then we can not release it?
				
		///////////////////////////////////////////////////
		//if sent tail matches the current head then this pipe has nothing in flight and can be re-assigned
		if (pipeIdx>=0 && (Pipe.headPosition(output[pipeIdx]) == pos)) {
		//	logger.info("NEW RELEASE for pipe {} connection {} at pos {}",pipeIdx, idToClear, pos);
			responsePipeLinePool.release(idToClear);
			
			assert( 0 == Pipe.releasePendingByteCount(output[pipeIdx]));
						
			if (id == ReleaseSchema.MSG_RELEASEWITHSEQ_101) {				
				BaseConnection conn = coordinator.lookupConnectionById(idToClear);
				if (null!=conn) {					
					conn.setSequenceNo(seq);//only set when we release a pipe
					conn.clearPoolReservation();
				}				
			} 
		}
	}

    private boolean hasNewDataToRead() {
    	
    	//assert(null==selectedKeys || selectedKeys.isEmpty()) : "All selections should be processed";
    		
        try {
        	////////////
        	//CAUTION - select now clears pevious count and only returns the additional I/O opeation counts which have become avail since the last time SelectNow was called
        	////////////        	
            if (selector.selectNow() > 0) {
            	selectedKeys = selector.selectedKeys();
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
    	ByteBuffer[] b = null;
    	long temp = 0;

		if (Pipe.hasRoomForWrite(targetPipe, reqPumpPipeSpace)) {

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
				b = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(targetPipe), 
                		                       targetPipe, 
                		                       readMaxSize);
                       
                assert(collectRemainingCount(b));
                
                temp = sourceChannel.read(b);
            	if (temp>0){
            		len+=temp;
            	}           

                assert(readCountMatchesLength(len, b));
                
                if (temp>=0 & cc!=null && cc.isValid && !cc.isDisconnecting()) { 
                
					if (len>0) {			
						return publishData(channelId, sequenceNo, targetPipe, len, b, true, newBeginning);
					} else {
						Pipe.unstoreBlobWorkingHeadPosition(targetPipe);
						return 1;
					}
                } else {
                
                	if (null!=cc) {
                		cc.clearPoolReservation();
                	}
					//logger.info("client disconnected, so release");
					//client was disconnected so release all our resources to ensure they can be used by new connections.
					selection.cancel();                	
					responsePipeLinePool.release(channelId);
					//to abandon this must be negative.
					len = -1;
					int result;
					if (len>0) {			
						result = publishData(channelId, sequenceNo, targetPipe, len, b, false, newBeginning);
					} else {
						result = abandonConnection(channelId, targetPipe, false, newBeginning);
					}
                	if (null!=cc) {
                		cc.close();
                	}
                	return result;
                }

            } catch (IOException e) {
            	
            	    logger.trace("client closed connection ",e.getMessage());
            	
	             	selection.cancel();                	
	            	responsePipeLinePool.release(cc.id);    
					cc.clearPoolReservation();
					boolean isOpen = temp>=0;
					int result;
					if (len>0) {			
						result = publishData(channelId, cc.getSequenceNo(), targetPipe, len, b, isOpen, newBeginning);
					} else {
						result = abandonConnection(channelId, targetPipe, isOpen, newBeginning);
					}            	
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
			Pipe.publishWrites(targetPipe);
			
		}				

		
		boolean fullTarget = b[0].remaining()==0 && b[1].remaining()==0;   

		Pipe.presumeRoomForWrite(targetPipe);
		
		if (messageType>=0) {
			
			publishSingleMessage(targetPipe, channelId, Pipe.unstoreBlobWorkingHeadPosition(targetPipe), len);     
			
			
		} else {
			final int size = Pipe.addMsgIdx(targetPipe, messageType);  
			Pipe.confirmLowLevelWrite(targetPipe, size);
		    Pipe.publishWrites(targetPipe);
		}
		
		
		//logger.info("{} wrote {} bytess to pipe {} return code: {}", System.currentTimeMillis(), len, targetPipe, ((fullTarget&&isOpen) ? 0 : 1));
		
		return (fullTarget && isOpen) ? 0 : 1; //only for 1 can we be sure we read all the data
	}

    private void publishSingleMessage(Pipe<NetPayloadSchema> targetPipe, 
		    		                 long channelId, 
		    		                 int pos, long remainLen) {

    	while (remainLen>0) {
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
