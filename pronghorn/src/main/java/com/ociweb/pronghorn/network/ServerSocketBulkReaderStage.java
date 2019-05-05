package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.SocketOption;
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

import com.ociweb.pronghorn.network.schema.SocketDataSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.SelectedKeyHashMapHolder;


public class ServerSocketBulkReaderStage extends PronghornStage {

		public static ServerSocketBulkReaderStage newInstance(GraphManager graphManager, Pipe<SocketDataSchema>[] output, 
    														ServerCoordinator coordinator) {
			return new ServerSocketBulkReaderStage(graphManager, output, coordinator);
		}
	
		protected ServerSocketBulkReaderStage(GraphManager graphManager, Pipe<SocketDataSchema>[] output, ServerCoordinator coordinator) {
			super(graphManager, NONE, output);
			
	        this.coordinator = coordinator;
	       
	        this.label = "\n"+coordinator.host()+":"+coordinator.port()+"\n";
	        
	        this.output = output;

	        coordinator.setStart(this);
	        
	        GraphManager.addNota(graphManager, GraphManager.PRODUCER, GraphManager.PRODUCER, this);
	        GraphManager.addNota(graphManager, GraphManager.LOAD_BALANCER, GraphManager.LOAD_BALANCER, this);
	        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
	        GraphManager.addNota(graphManager, GraphManager.DOT_RANK_NAME, "SocketReader", this);
	        
	        Number dsr = graphManager.defaultScheduleRate();
	        if (dsr!=null) {
	        	GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, dsr.longValue()/2, this);
	        }
	               
			//        //If server socket reader does not catch the data it may be lost
			//        GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
	      
	        //if too slow we may loose data but if too fast we may starve others needing data
	        //GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, 100_000L, this);
	      
	        GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);	       
	        //much larger limit since nothing needs this thread back.
	        GraphManager.addNota(graphManager, GraphManager.SLA_LATENCY, 100_000_000, this);
	        
	        

	        
	        try {//must be done early to ensure this is ready before the other stages startup.
	        	coordinator.registerSelector(selector = Selector.open());
	        } catch (IOException e) {
	        	throw new RuntimeException(e);
	        }
	        
//			long gt = NetGraphBuilder.computeGroupsAndTracks(coordinator.moduleParallelism(), coordinator.isTLS);		 
//			int groups = (int)((gt>>32)&Integer.MAX_VALUE);//same as count of SocketReaders
//			int tracks = (int)gt&Integer.MAX_VALUE; //tracks is count of HTTP1xRouters
//		

		}
	   
		private final static Logger logger = LoggerFactory.getLogger(ServerSocketBulkReaderStage.class);
		
		private static final int messageType = SocketDataSchema.MSG_DATA_210;;
		private static final int singleMessageSpace = Pipe.sizeOf(SocketDataSchema.instance, messageType);
		private static final int reqPumpPipeSpace = Pipe.sizeOf(SocketDataSchema.instance, messageType);

	    private final Pipe<SocketDataSchema>[] output;
	
	    private final ServerCoordinator coordinator;

	    private final Selector selector;
	
	    
	    public static boolean showRequests = false;
	    
	    private boolean shutdownInProgress;
	    private final String label;
	    private Set<SelectionKey> selectedKeys;	
	    private ArrayList<SelectionKey> doneSelectors = new ArrayList<SelectionKey>(100);
	    	        
	    @Override
	    public String toString() {
	    	return super.toString()+label;
	    }

	    private SocketOption<Boolean> TCP_QUICKACK_LOCAL;
	    
	    @Override
	    public void startup() {
	    	
			//look this up once early in case we are running on Java 10+.
	    	try {
	    		Class<?> clazz = Class.forName("jdk.net.ExtendedSocketOptions");
	    		Field field = clazz.getDeclaredField("TCP_QUICKACK");
	    		
	    		//System.out.println(Arrays.deepToString(clazz.getDeclaredFields()));
	    		
	    		TCP_QUICKACK_LOCAL = (SocketOption<Boolean>)field.get(null);
	    
	    	} catch (Throwable t) {
	    		//ignore, not supported on this platform
	    	}
			
	    	this.selectedKeyHolder = new SelectedKeyHashMapHolder();
			
	        ServerCoordinator.newSocketChannelHolder(coordinator);
 
	      
	    }
	    
	    @Override
	    public void shutdown() {
	    	Pipe.publishEOF(output);	       
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

	    @Override
	    public void run() {
	 
	    	 if(!shutdownInProgress) {
	 	        	
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

		    	            removeDoneKeys(selectedKeys);
		    	            
		    	            if (!hasRoomForMore) {
		    	            	return;
		    	            } 
		    	        }
		    	       
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
			
			final long channelId = ((ConnectionContext)selection.attachment()).getChannelId();
			assert(channelId>=0);
			//logger.info("\nnew key selection in reader for connection {}",channelId);
			
			return processSelectionImpl(selection, channelId, 
					                    coordinator.lookupConnectionById(channelId), 
					                    output[(int)channelId%output.length]);

		}

		private boolean processSelectionImpl(SelectionKey selection, 
											final long channelId, BaseConnection cc,
											Pipe<SocketDataSchema> pipe) {
			if (null != cc) {
				if (!coordinator.isTLS) {	
					return (pumpByteChannelIntoPipe(this, pipe, cc)==1);
				} else {
					handshakeTaskOrWrap(cc);
					return (pumpByteChannelIntoPipe(this, pipe, cc)==1);
				}		
			} else {				
				return processClosedConnection((SocketChannel) selection.channel(), channelId, pipe);
			}
		}
		
		private static void handshakeTaskOrWrap(BaseConnection cc) {
			if (null!=cc && null!=cc.getEngine()) {
				 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();
				 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
			            Runnable task;//TODO: there is an opportunity to have this done by a different stage in the future.
			            while ((task = cc.getEngine().getDelegatedTask()) != null) {
			            	task.run();
			            }
				 } 			 
			}
		}
		

		private static boolean processClosedConnection(final SocketChannel socketChannel, long channelId, Pipe<SocketDataSchema> outputPipe) {

			assert(validateClose(socketChannel, channelId));
			try {
				socketChannel.close();
			} catch (IOException e) {				
			}
			//if this selection was closed then remove it from the selections
			
//TODO: send a close??
			//if (PoolIdx.getIfReserved(responsePipeLinePool,channelId)>=0) {
			//	responsePipeLinePool.release(channelId);
			//}			

			//doneSelectors.add(selection);//remove them all.
			return true;
		} 

			
		////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////

		private static boolean validateClose(final SocketChannel socketChannel, final long channelId) {

			try {
				int len = socketChannel.read(ByteBuffer.allocate(3));
				
				//if (len>0) {
				//	logger.trace("client connection is sending addional data after closing connection: {}",socketChannel);
				//}
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


		private boolean hasNewDataToRead(Selector selector) {
	    	
	    	//assert(null==selectedKeys || selectedKeys.isEmpty()) : "All selections should be processed";
//	    	if (null!=selectedKeys && !selectedKeys.isEmpty()) {
//	    		return true; //keep this!
//	    	}
	    	
	        try {
	                	
	        	////////////
	        	//CAUTION - select now clears previous count and only returns the additional I/O operation counts which have become avail since the last time SelectNow was called
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
	            
	            shutdownInProgress = true;
	            return false;
	        }
	    }

	    private int r1; //needed by assert
	    private int r2; //needed by assert
	    
	    //returns -1 for did not start, 0 for started, and 1 for finished all.
	    private static int pumpByteChannelIntoPipe(
	    		final ServerSocketBulkReaderStage that,
	    		final Pipe<SocketDataSchema> outputPipe, final BaseConnection cc) {

	    	
	    	//keep appending messages until the channel is empty or the pipe is full
	    	long len = 0;//if data is read then we build a record around it
	    	ByteBuffer[] targetBuffer = null;
	    	long temp = 0;

			if (Pipe.hasRoomForWrite(outputPipe, reqPumpPipeSpace)) {
				//only call nano if we are consuming this one.
				cc.setLastUsedTime(System.nanoTime());//needed to know when this connection can be disposed
				
	            try {                
	                
	            	//Read as much data as we can...
	            	//We must make the writing buffers larger based on how many messages we can support
	            	int readMaxSize = outputPipe.maxVarLen;           	
	            	long units = outputPipe.sizeOfSlabRing - (Pipe.headPosition(outputPipe)-Pipe.tailPosition(outputPipe));
	            	units -= reqPumpPipeSpace;
	            	if (units>0) {
		            	int extras = (int)units/singleMessageSpace;
		            	if (extras>0) {
		            		readMaxSize += (extras*outputPipe.maxVarLen);
		            	}
	            	}

	                //NOTE: the byte buffer is no longer than the valid maximum length but may be shorter based on end of wrap around
					int wrkHeadPos = Pipe.storeBlobWorkingHeadPosition(outputPipe);
					targetBuffer = Pipe.wrappedWritingBuffers(wrkHeadPos, 
												outputPipe, readMaxSize);
	                       
	                assert(that.collectRemainingCount(targetBuffer));
	  
	                //read as much as we can, one read is often not enough for high volume
	                boolean isStreaming = false; //TODO: expose this switch..
	                
	                SocketChannel sourceChannel = cc.getSocketChannel();
	                do {
	                	temp = sourceChannel.read(targetBuffer);
	                	if (temp>0){
	                		len+=temp;
	                	}
	                	
	                } while (temp>0 && isStreaming); //for multiple in flight pipelined must keep reading...
	                
	                //784 needed for 16,  49 byes per request
	                //System.out.println(len); ServerSocketReaderStage.showRequests=true;
	                
	                try {    
	    				if (null!=that.TCP_QUICKACK_LOCAL) {
	    					//only for 10+ ExtendedSocketOptions.TCP_QUICKACK
	    					sourceChannel.setOption(that.TCP_QUICKACK_LOCAL, Boolean.TRUE);
	    				}
	    			} catch (IOException e1) {
	    				//NOTE: may not be supported on on platforms so ignore this 
	    			}	
	        
	                
	                assert(that.readCountMatchesLength(len, targetBuffer));
	                
//	                if (temp<=0) {
//	                	doneSelectors.add(selection);
//	                }
	                if (temp>=0 & cc!=null && cc.isValid() && !cc.isDisconnecting()) { 
	                
	                	
						if (len>0) {
							return that.publishData(cc.id, cc.getSequenceNo(), outputPipe, len, targetBuffer, true);
						} else {
							Pipe.unstoreBlobWorkingHeadPosition(outputPipe);
							return 1;
						}
	                } else {
	                	//logger.info("client disconnected, so release");
	                
	                	if (null!=cc) {
	                		cc.clearPoolReservation();
	                	}
						//client was disconnected so release all our resources to ensure they can be used by new connections.
	               	
						//to abandon this must be negative.				
						int result = that.abandonConnection(cc.id, outputPipe, false);
						
	                	if (null!=cc) {
	                		cc.close();
	                	}
	                	return result;
	                }

	            } catch (IOException e) {
	            	
	            	    //logger.trace("client closed connection ",e.getMessage());
	            	
						boolean isOpen = temp>=0;
						int result;
						if (len>0) {			
							result = that.publishData(cc.id, cc.getSequenceNo(), outputPipe, len, targetBuffer, isOpen);
						} else {
							result = that.abandonConnection(cc.id, outputPipe, isOpen);
						}          	
	              	   
						cc.clearPoolReservation(); //TODO: should this be here??
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

		private int abandonConnection(long channelId, Pipe<SocketDataSchema> targetPipe, boolean isOpen) {
			//logger.info("{} abandon one record, did not publish because length was {}    {}",System.currentTimeMillis(), len,targetPipe);

			 Pipe.unstoreBlobWorkingHeadPosition(targetPipe);//we did not use or need the writing buffers above.
			 
						 
			 return 1;//yes we are done
		}

		private int publishData(long channelId, int sequenceNo, Pipe<SocketDataSchema> target, long len, ByteBuffer[] b,
				boolean isOpen) {
					
			Pipe.presumeRoomForWrite(target);
			
			if (messageType>=0) {
				
				len = publishSingleMessage(target,
						             channelId, 
						             Pipe.unstoreBlobWorkingHeadPosition(target), len);     
				
				
			} else {
				Pipe.publishEOF(target);
				
//				final int size = Pipe.addMsgIdx(targetPipe, messageType);  
//				Pipe.confirmLowLevelWrite(targetPipe, size);
//			    Pipe.publishWrites(targetPipe);
			}
			
			
			//logger.info("{} wrote {} bytess to pipe {} return code: {}", System.currentTimeMillis(), len, targetPipe, ((fullTarget&&isOpen) ? 0 : 1));
			
			return ((len!=0) && isOpen) ? 0 : 1; //only for 1 can we be sure we read all the data
		}
		
	    private long publishSingleMessage(Pipe<SocketDataSchema> targetPipe, 
			    		                 long channelId, 
			    		                 int pos, long remainLen) {
	    	
	    	while (remainLen>0 && Pipe.hasRoomForWrite(targetPipe)) {
	    		
	    		    int localLen = (int)Math.min(targetPipe.maxVarLen, remainLen);     
	    		    
		            final int size = Pipe.addMsgIdx(targetPipe, messageType); //SocketDataSchema.MSG_DATA_210 ;             
		            Pipe.addLongValue(channelId, targetPipe);  
		            Pipe.addLongValue(System.nanoTime(), targetPipe);
			        Pipe.addLongValue(0, targetPipe); //TODO: hash space holder. NOTE: upgrade this
					
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
	    	return remainLen;
	    }

		private void showRequests(Pipe<SocketDataSchema> targetPipe, long channelId, int pos, int localLen) {
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
