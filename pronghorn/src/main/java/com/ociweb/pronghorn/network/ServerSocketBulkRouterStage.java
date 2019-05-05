package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.network.schema.SocketDataSchema;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.PoolIdx;
import com.ociweb.pronghorn.util.PoolIdxPredicate;

/**
 * Server-side stage that reads from the socket. Useful for building a server.
 * Accepts unexpected calls (unlike ClientSocketReaderStage).
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ServerSocketBulkRouterStage extends PronghornStage {
   
	private final int messageType;
	private final int singleMessageSpace;
	private final int reqPumpPipeSpace;

	public static final Logger logger = LoggerFactory.getLogger(ServerSocketBulkRouterStage.class);
    
	private final Pipe<SocketDataSchema> input;
    private final Pipe<NetPayloadSchema>[] output;
    private final Pipe<ReleaseSchema>[] releasePipes;
    private final ServerCoordinator coordinator;

    public static boolean showRequests = false;
    
    private boolean shutdownInProgress;
    private final String label;

	private PoolIdx responsePipeLinePool;
	private final int tracks;
	
    
    public static ServerSocketBulkRouterStage newInstance(GraphManager graphManager, Pipe<SocketDataSchema> input, Pipe<ReleaseSchema>[] ack, Pipe<NetPayloadSchema>[] output, 
    		                                          ServerCoordinator coordinator) {
        return new ServerSocketBulkRouterStage(graphManager, input, ack, output, coordinator);
    }

	/**
	 *
	 * @param graphManager
	 * @param ack _in_ The release acknowledgment.
	 * @param output _out_ The read payload from the socket.
	 * @param coordinator
	 */
	public ServerSocketBulkRouterStage(GraphManager graphManager, Pipe<SocketDataSchema> input,
			                           Pipe<ReleaseSchema>[] ack, Pipe<NetPayloadSchema>[] output,
			                           ServerCoordinator coordinator) {
        super(graphManager, join(input, ack), output);
        this.coordinator = coordinator;
       
        this.label = "\n"+coordinator.host()+":"+coordinator.port()+"\n";
        
        this.output = output;
        this.input = input;
        this.releasePipes = ack;

        this.messageType = coordinator.isTLS ? NetPayloadSchema.MSG_ENCRYPTED_200 : NetPayloadSchema.MSG_PLAIN_210;
        coordinator.setStart(this);
        

        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
        GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketRouter", this);
        
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
      
		long gt = NetGraphBuilder.computeGroupsAndTracks(coordinator.moduleParallelism(), coordinator.isTLS);		 
		int groups = (int)((gt>>32)&Integer.MAX_VALUE);//same as count of SocketReaders
		tracks = (int)gt&Integer.MAX_VALUE; //tracks is count of HTTP1xRouters
    }
        
    @Override
    public String toString() {
    	return super.toString()+label;
    }

    @Override
    public void startup() {
		
    	this.responsePipeLinePool = new PoolIdx(output.length, 1);
             
      
    }
    
    
    @Override
    public void shutdown() {
    	Pipe.publishEOF(output);  
    	
    //	System.out.println("LOCK STATUS:\n"+responsePipeLinePool);
    	
    }
    
    boolean showLocks = false;
    long lastTime = 0;
    
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
    	    	int maxConsume = 1+(output.length>>1);//closed loop check
    	    	do {
    	    		
    	    		
    	    		
	    	        if (Pipe.hasContentToRead(input) && (--maxConsume>=0)) { 
	    	        	iter = 1;
	       
	    	        	//TODO: we already read but we do not know if we have room!!!
	    	        	int msgIdx = Pipe.peekInt(input); 
	    	        			
	    	        	
	    	        	if (msgIdx == SocketDataSchema.MSG_DISCONNECT_203) {
	    	        		
	    	        		Pipe.takeMsgIdx(input);
	    	        		long channelId = Pipe.takeLong(input);
	    	        		
	    	        		System.out.println("send out disconnect");
	    	        		
	    	        		Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
							Pipe.releaseReadLock(input);
							
	    	        	} else {
	    	 
	    	        		if (msgIdx != -1) {
	    	        			
	    	        			if (msgIdx != SocketDataSchema.MSG_DATA_210) {
	    	        				throw new UnsupportedOperationException("unknown msgId: "+msgIdx);
	    	        			}
	
	    	        			long channelId = Pipe.peekLong(input, 0xFF & SocketDataSchema.MSG_DATA_210_FIELD_CONNECTIONID_201);
      	        	
								assert(channelId>=0);
								BaseConnection cc = coordinator.lookupConnectionById(channelId);
								
								//logger.info("\nnew key selection in reader for connection {}",channelId);
								
								if (null != cc) {
									if (!coordinator.isTLS) {	
									} else {
										handshakeTaskOrWrap(cc);
									}		
									
									//********************* URGENT FIX...
									//TODO: not sure pump always works with pipe out size vs in size..
									if (processConnection2(cc, input)) {
									
										Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
										Pipe.releaseReadLock(input);
									
										showLocks = true;
										lastTime = System.currentTimeMillis();
									} else {
										iter--;
									}
								} else {
									Pipe.skipNextFragment(input);
									//if this selection was closed then remove it from the selections
									
									if (PoolIdx.getIfReserved(responsePipeLinePool,channelId)>=0) {
										responsePipeLinePool.release(channelId);
									}
								}
			
		    	        	} else {
		    	        		Pipe.skipNextFragment(input);
		    	        		//got the close end of stream message
		    	        	}
	    	        	}
	    	        }
	    	        releasePipesForUse();//clear now so select will pick up more
	    	        
	    	       // System.out.println("iter: "+iter+"  "+Pipe.hasContentToRead(input));
	    	        
    	    	} while (--iter>=0);
    	   
//    	    	//show 10 sec after no activity.
//    	    	if (showLocks && System.currentTimeMillis()>(lastTime+10_000)) {
//    	    		System.out.println("LOCK STATUS:\n"+responsePipeLinePool);
//    	    		showLocks = false;
//    	    	}
    	 
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
  

	private boolean processConnection2(BaseConnection cc, Pipe<SocketDataSchema> input) {
		

		int responsePipeLineIdx = cc.getPoolReservation();
		final boolean newBeginning = responsePipeLineIdx<0;
		
		if (newBeginning) {
			responsePipeLineIdx = beginningProcessing(cc.id, cc);

			if (responsePipeLineIdx >= 0) {	
				assert(output[responsePipeLineIdx].maxVarLen >= input.maxVarLen) : "out: "+output[responsePipeLineIdx].maxVarLen+" in: "+input.maxVarLen;
				//System.out.println("new lock "+cc.id+"  "+responsePipeLineIdx);
				return (pumpByteChannelIntoPipe(input, cc.id, 
						cc.getSequenceNo(), output[responsePipeLineIdx], 
						newBeginning, cc)==1);
			} else {
				
				//TODO: if this happens many times in a row here we should find the oldest pipe which only has partial
				//      data and close it, it is probably a bad client.
				
				
				//unable to find an open pipe so work later...
				return false;
			}
		} else {			
			
			return (pumpByteChannelIntoPipe(input, cc.id, 
					cc.getSequenceNo(), output[responsePipeLineIdx], 
					newBeginning, cc)==1);			
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
			return responsePipeLinePool.get(channelId, 0);	
			
		} else {
			
			releaseOldChannelId(channelId);
			//this release is required in case we are swapping pipe lines, we ensure that the latest sequence no is stored.
			releasePipesForUse();
		
			//key & group to ensure connection is sent to same track	
			return responsePipeLinePool.get(channelId, 0);	
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
			System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
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
				
		//System.out.println("relase connection: "+idToClear);
		
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
			if (pipeIdx>=0 && Pipe.workingHeadPosition(output[pipeIdx])<=pos && (Pipe.headPosition(output[pipeIdx]) == pos)	) {
	       	    releaseIdx(id, idToClear, seq, cc, pipeIdx); 	
	      // 	     System.out.println("A did clear "+idToClear+"  "+pipeIdx);
			} else {
		//		System.out.println("A not cleared "+idToClear+"  "+pipeIdx);
			}
		} else {
			int pipeIdx = (providedPipeIdx>=0) ? providedPipeIdx : PoolIdx.getIfReserved(responsePipeLinePool, idToClear);
			if (pipeIdx!=-1) {
				PoolIdx.release(responsePipeLinePool, idToClear, pipeIdx);
			//	System.out.println("B did clear "+idToClear+"  "+pipeIdx);
			} else {
			//	System.out.println("B not cleared "+idToClear+"  "+pipeIdx);
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
	

    
    //returns -1 for did not start, 0 for started, and 1 for finished all.
    private int pumpByteChannelIntoPipe(Pipe<SocketDataSchema> input,
    		long channelId, int sequenceNo, Pipe<NetPayloadSchema> targetPipe, 
    		boolean newBeginning, BaseConnection cc) {

    	//keep appending messages until the channel is empty or the pipe is full
   
    	long temp = 0;
    	
		if (Pipe.hasRoomForWrite(targetPipe, reqPumpPipeSpace)) {
			
			//we have room so now read the data...
			
			Pipe.takeMsgIdx(input);
        	long channelId2 = Pipe.takeLong(input);
        	assert(channelId == channelId2) : "internal error";
        	long arrivalTime = Pipe.takeLong(input);
        	long hash = Pipe.takeLong(input);
        	//TODO: do something with this hash value...
        	ChannelReader reader = Pipe.openInputStream(input);
        	long len = reader.available();//if data is read then we build a record around it
   		    	
        	//TODO: Needed since we set already in previous stage?????        	
			cc.setLastUsedTime(arrivalTime);//needed to know when this connection can be disposed
			
            {                
                
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
				
				//direct copy into target blob buffer
				reader.readInto(Pipe.blob(targetPipe), wrkHeadPos, readMaxSize, Pipe.blobMask(targetPipe));

                if (temp>=0 & cc!=null && cc.isValid() && !cc.isDisconnecting()) { 

					if (len>0) {
						int result = publishData(channelId, sequenceNo, targetPipe, len, newBeginning)
								? 1 : 0; //if all published we return 1
						if (0==result) {
	                		throw new UnsupportedOperationException();
	                	}
						
						return result;
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
               	
					responsePipeLinePool.release(channelId);
					//to abandon this must be negative.				
					int result = abandonConnection(channelId, targetPipe, false, newBeginning);
					
                	if (null!=cc) {
                		cc.close();
                	}
                	if (0==result) {
                		throw new UnsupportedOperationException();
                	}
                	return result;
                }

            }
        } else {
        	//logger.info("\ntry again later, unable to launch do to lack of room in {} ",targetPipe);
        	return -1;
        }
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

	private boolean publishData(long channelId, int sequenceNo, Pipe<NetPayloadSchema> targetPipe, long len, 
						    boolean newBeginning) {
		if (newBeginning) {	
										
			Pipe.presumeRoomForWrite(targetPipe);
			
			int size = Pipe.addMsgIdx(targetPipe, NetPayloadSchema.MSG_BEGIN_208);
			Pipe.addIntValue(sequenceNo, targetPipe);						
			Pipe.confirmLowLevelWrite(targetPipe, size);
			Pipe.batchFollowingPublishes(targetPipe, 1);
			
			Pipe.publishWrites(targetPipe); //wait on publish until the rest is ready...
			
		}				

		Pipe.presumeRoomForWrite(targetPipe);
		
		if (messageType>=0) {
			//true if all written
			//System.out.println("publishSingleMessage "+channelId);
			return publishSingleMessage(targetPipe, channelId, Pipe.unstoreBlobWorkingHeadPosition(targetPipe), len);     

		} else {
			Pipe.publishEOF(targetPipe);
			return len==0;
		}

	}
	
    private boolean publishSingleMessage(Pipe<NetPayloadSchema> targetPipe, 
		    		                 long channelId, 
		    		                 int pos, long remainLen) {
    	
    	while (remainLen>0 && Pipe.hasRoomForWrite(targetPipe)) {

    		//System.out.println("publish to channel "+channelId);
    		
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
    	Pipe.publishAllBatchedWrites(targetPipe); //Ensure we have nothing waiting, key for watchers.
    	
    	if (remainLen>0) {
    		new Exception("unable to write all "+remainLen).printStackTrace();
    	}
    	
    	return remainLen==0; //true if all written
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
