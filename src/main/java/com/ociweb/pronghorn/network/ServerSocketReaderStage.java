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

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ReleaseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
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

    private Selector selector;

    private int pendingSelections = 0;
    private final boolean isTLS;

    private StringBuilder[] accumulators;
    

    private int maxWarningCount = 20;
    
    private ArrayList<SelectionKey> doneSelectors= new ArrayList<SelectionKey>(100);
    
//    private long nextTime = 0;
//    private long bytesConsumed=0;

    private ServiceObjectHolder<ServerConnection> holder;
    
    public static ServerSocketReaderStage newInstance(GraphManager graphManager, Pipe<ReleaseSchema>[] ack, Pipe<NetPayloadSchema>[] output, ServerCoordinator coordinator, boolean encrypted) {
        return new ServerSocketReaderStage(graphManager, ack, output, coordinator, encrypted);
    }
    
    public ServerSocketReaderStage(GraphManager graphManager, Pipe<ReleaseSchema>[] ack, Pipe<NetPayloadSchema>[] output, ServerCoordinator coordinator, boolean isTLS) {
        super(graphManager, ack, output);
        this.coordinator = coordinator;

        this.output = output;
        this.releasePipes = ack;
        this.isTLS = isTLS;
        this.messageType = isTLS ? NetPayloadSchema.MSG_ENCRYPTED_200 : NetPayloadSchema.MSG_PLAIN_210;
        coordinator.setStart(this);
        
        GraphManager.addNota(graphManager, GraphManager.PRODUCER, GraphManager.PRODUCER, this);
        
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
		
        holder = ServerCoordinator.newSocketChannelHolder(coordinator);
                
        try {
            coordinator.registerSelector(selector = Selector.open());
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        //logger.debug("selector is registered for pipe {}",pipeIdx);
        
        
    }
    
    @Override
    public void shutdown() {
        int i = output.length;
        while (--i >= 0) {
        	if (Pipe.isInit(output[i])) {
        		Pipe.spinBlockForRoom(output[i], Pipe.EOF_SIZE);
            	Pipe.publishEOF(output[i]);  
        	}
        }
        logger.warn("server reader has shut down");
    }

    
    private final Consumer<SelectionKey> selectionKeyAction = new Consumer<SelectionKey>(){
			@Override
			public void accept(SelectionKey selection) {
				processSelection(selection); 
			}
    };    
    
    @Override
    public void run() {
        
//    	long now = System.currentTimeMillis();
//    	if (now>nextTime) {    		
//    		//if one backs up we will never read the others... TOOD: this is very bad... Urgent, bad connection must be killed before stopping others.
//    		System.err.println("Server Socket read "+bytesConsumed+" selector size "+selectorSize+" pending "+pendingSelections);  //TODO: we stopped reading data so the client stops sending it.  		
//    		nextTime = now+3_000;
//    	}
    	
    		{	
    	
	        ////////////////////////////////////////
	        ///Read from socket
	        ////////////////////////////////////////

    	    //max cycles before we take a break.
	    	int maxIterations = 100; //important or this stage will take all the resources.
	    	
	        while (--maxIterations>=0 && hasNewDataToRead()) {
	        	
	        	//logger.info("found new data to read on "+groupIdx);
	            
	           Set<SelectionKey> selectedKeys = selector.selectedKeys();
	            
	           assert(selectedKeys.size()>0);	            
	           assert(pendingSelections == selectedKeys.size());
	           doneSelectors.clear();
		
	           selectedKeys.forEach(selectionKeyAction);
	            
//	           for (SelectionKey selection: selectedKeys) {
//	                processSelection(selection); 
//	            }	            

			   //sad but this is the best way to remove these without allocating a new iterator
			   	// the selectedKeys.removeAll(doneSelectors); will produce garbage upon every call
			   	int c = doneSelectors.size();
			   	while (--c>=0) {
			        		selectedKeys.remove(doneSelectors.get(c));
			    }

		      	 assert(pendingSelections == selectedKeys.size());
			        	 
	        }	        
    	}
    }

	private void processSelection(SelectionKey selection) {
		assert(0 != (SelectionKey.OP_READ & selection.readyOps())) : "only expected read"; 
		SocketChannel socketChannel = (SocketChannel)selection.channel();
         
		//logger.info("is blocking {} open {} ", selection.channel().isBlocking(),socketChannel.isOpen());
		
		
		//get the context object so we know what the channel identifier is
		ConnectionContext connectionContext = (ConnectionContext)selection.attachment();                
		final long channelId = connectionContext.getChannelId();
						
		
		SSLConnection cc = coordinator.get(channelId);
		boolean processWork = true;
		if (isTLS) {
				
			if (null!=cc && null!=cc.getEngine()) {
				HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();

				 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
		                Runnable task;//TODO: there is anopporuntity to have this done by a different stage in the future.
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
						//coordinator.checkForResponsePipeLineIdx(channelId);	
				
				
				final boolean newBeginning = (responsePipeLineIdx<0);
						
				if (newBeginning) {
					//this release is required in case we are swapping pipe lines, we ensure that the latest sequence no is stored.
					releasePipesForUse();
					responsePipeLineIdx = coordinator.responsePipeLineIdx(channelId);
					if (-1 == responsePipeLineIdx) { //handshake is dropped by input buffer at these loads?
						releasePipesForUse();
						responsePipeLineIdx = coordinator.responsePipeLineIdx(channelId);
					}
					if (responsePipeLineIdx>=0) {
						cc.setPoolReservation(responsePipeLineIdx);
					}
				}
					
				if (responsePipeLineIdx>=0) {
					int pumpState = pumpByteChannelIntoPipe(socketChannel, channelId, output[responsePipeLineIdx], newBeginning, cc, selection); 
		            if (pumpState>0) {
		            	assert(1==pumpState) : "Can only remove if all the data is known to be consumed";
		            	doneSelectors.add(selection);//add to list for removal
		            	pendingSelections--;
		            }
		          
		            if ((++rMask&0x3F)==0) {
		             releasePipesForUse(); //must run but not on every pass
		            }
				}
		        
		}
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
	    			
	    			long idToClear = Pipe.takeLong(a);
	    			long pos = Pipe.takeLong(a);	    			
	    			int seq = Pipe.takeInt(a);	    				    			
	    			releaseIfUnused(msgIdx, idToClear, pos, seq);
	    			Pipe.confirmLowLevelRead(a, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASEWITHSEQ_101));
	    			
	    		} else if (msgIdx == ReleaseSchema.MSG_RELEASE_100) {
	    			
	    			logger.info("warning, legacy (client side) release use detected in the server.");
	    			
	    			long idToClear = Pipe.takeLong(a);
	    			long pos = Pipe.takeLong(a);	    					
	    			releaseIfUnused(msgIdx, idToClear, pos, -1);
	    			Pipe.confirmLowLevelRead(a, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASE_100));
	    			
	    		} else {
	    			logger.info("unknown or shutdown on release");
	    			assert(-1==msgIdx);
	    			//requestShutdown(); //TODO: we should not shutdown if release is the end?
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
				SSLConnection conn = coordinator.get(idToClear);
				if (null!=conn) {					
					conn.setSequenceNo(seq);//only set when we release a pipe
				}
				conn.clearPoolReservation();
				
			} else {
				logger.info("legacy release detected, error...");
			}
			
		} else {
			//logger.info("SKIP RELEASE for pipe {} connection {}",pipeIdx, idToClear);
			
			if (pipeIdx>=0) {
				
			//	logger.info(pipeIdx+"  no release for pipe {} release head position {}",output[pipeIdx],pos); //what about EOF is that blocking the relase.

				if (pos>Pipe.headPosition(output[pipeIdx])) {
					System.err.println("EEEEEEEEEEEEEEEEEEEEEEEEe  got ack but did not release on server. pipe "+pipeIdx+" pos "+pos+" expected "+Pipe.headPosition(output[pipeIdx]) );
					//System.exit(-1);
				} else {
					//this is the expected case where more data came in for this pipe
				}
			//	throw new UnsupportedOperationException("not released pos did not match "+ pos+" in "+output[pipeIdx]);
				
			} else {
				//probably already cleared and closed
				logger.trace("WARNING: not released, could not find "+idToClear);
			}
		}
	}

    private boolean hasNewDataToRead() {
    	
    	if (pendingSelections>0) {
    		return true;
    	}
    	
    	assert (0 == selector.selectedKeys().size());
    	    	
    	
        try {        	        	
        	/////////////
        	//CAUTION - select now clears pevious count and only returns the additional I/O opeation counts which have become avail since the last time SelectNow was called
        	////////////        	
            pendingSelections=selector.selectNow();
        //    logger.info("pending new selections {} ",pendingSelections);
            return pendingSelections>0;
        } catch (IOException e) {
            logger.error("unexpected shutdown, Selector for this group of connections has crashed with ",e);
            requestShutdown();
            return false;
        }
    }
    
    
    
    //returns -1 for did not start, 0 for started, and 1 for finished all.
    public int pumpByteChannelIntoPipe(SocketChannel sourceChannel, long channelId, Pipe<NetPayloadSchema> targetPipe, boolean newBeginning, SSLConnection cc, SelectionKey selection) {
    	
        //keep appending messages until the channel is empty or the pipe is full
        if (Pipe.hasRoomForWrite(targetPipe)) {          
        	//logger.info("pump block for {} ",channelId);
            try {                
                
                long len=0;//if data is read then we build a record around it
                //NOTE: the byte buffer is no longer than the valid maximum length but may be shorter based on end of wrap arround
                ByteBuffer[] b = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(targetPipe), targetPipe);
                       
                int r1 = b[0].remaining();
                int r2 = b[1].remaining();
                
                final long temp = sourceChannel.read(b);
                
//            	tempBuf.clear();
//            	final long temp = sourceChannel.read(tempBuf);
//            	tempBuf.flip(); 	
//            	int space = b[0].remaining();
//            	if (temp<=space) {
//            		b[0].put(tempBuf);
//            	} else {
//            		int limit = tempBuf.limit();                	
//            		tempBuf.limit(tempBuf.position()+space);
//            		b[0].put(tempBuf);
//            		tempBuf.limit(limit);
//            		b[1].put(tempBuf);
//            		
//            	}
            	
            	
            	if (temp>0){
            		len+=temp;
            	}            
                 
                int readCount = (r1-b[0].remaining())+(r2-b[1].remaining());
                assert(readCount == len) : "server "+readCount+" vs "+len;
                
                if (temp<0) {
                	logger.info("client disconnected, so release");
                	//client was disconnected so release all our resources to ensure they can be used by new connections.
                	selection.cancel();                	
                	coordinator.releaseResponsePipeLineIdx(cc.id);    
    				cc.clearPoolReservation();
                	
                }
                return publishOrAbandon(channelId, targetPipe, len, b, temp>=0, newBeginning, cc);

            } catch (IOException e) {
            	
            		this.coordinator.releaseResponsePipeLineIdx(channelId);
            	
                    recordErrorAndClose(sourceChannel, channelId, e);
                        
                    return -1;
            }
        } else {

        	return -1;
        }
    }

	private int publishOrAbandon(long channelId, Pipe<NetPayloadSchema> targetPipe, long len, ByteBuffer[] b, boolean isOpen, boolean newBeginning, SSLConnection cc) {
		if (len>0) {
			
			if (newBeginning) {	
								
			//	logger.info("NEW ALLOCATION OF PIPE for connection {}     {}",  cc.id, targetPipe );
				
				if (Pipe.hasRoomForWrite(targetPipe)) {//WARNING we have mixed two messages here !!! we wrote the bytes for the next already 
								
					int size = Pipe.addMsgIdx(targetPipe, NetPayloadSchema.MSG_BEGIN_208);
					Pipe.addIntValue(cc.getSequenceNo(), targetPipe);						
					Pipe.confirmLowLevelWrite(targetPipe, size);
					Pipe.publishWrites(targetPipe);
							
				} else {
					//TODO: make this an assert....
					logger.info("internal error, picked up new pipe but it has data {}",targetPipe);
					throw new UnsupportedOperationException();
				}
			}				
			
		//	logger.info("normal publish for connection {}     {}     {} channelID {} ",  cc.id, targetPipe, messageType, channelId );
			assert(cc.id == channelId) : "should match "+cc.id+" vs "+channelId;
			
			boolean fullTarget = b[0].remaining()==0 && b[1].remaining()==0;   
//			bytesConsumed+=len;
			publishData(targetPipe, channelId, len, cc);                  	 
//			logger.info("wrote {} bytess to pipe {} ", len,targetPipe);
			return (fullTarget&&isOpen) ? 0 : 1; //only for 1 can we be sure we read all the data
		} else {
			 
			logger.info("abandon one record, did not publish because length was {}    {}",len,targetPipe);

			 Pipe.unstoreBlobWorkingHeadPosition(targetPipe);//we did not use or need the writing buffers above.
			 
             if (isOpen && newBeginning) { //Gatling does this a lot, TODO: we should optimize this case.
             	//we will abandon but we also must release the reservation because it was never used
             	coordinator.releaseResponsePipeLineIdx(channelId);
             	SSLConnection conn = coordinator.get(channelId);
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
//        boolean showRequests = false;
//        if (showRequests) {
//        	logger.info("//////////////////Server read for channel {} bPos{} len {} \n{}\n/////////////////////",channelId, originalBlobPosition, len, 
//        			Appendables.appendUTF8(new StringBuilder(), targetPipe.blobRing, originalBlobPosition, (int)len, targetPipe.blobMask));               
//        }
        
        
        Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int)len, targetPipe);  
        
        //all breaks are detected by the router not here
        //(section 4.1 of RFC 2616) end of header is \r\n\r\n but some may only send \n\n
        //
   
        Pipe.confirmLowLevelWrite(targetPipe, size);
        Pipe.publishWrites(targetPipe);
        
    }
    
    
	private void testValidContent(final int idx, Pipe<NetPayloadSchema> pipe, int pos, int len) {
		
		if (ServerCoordinator.TEST_RECORDS) {
			
			//write pipeIdx identifier.
			//Appendables.appendUTF8(System.out, target.blobRing, originalBlobPosition, readCount, target.blobMask);
		
			
			boolean confirmExpectedRequests = true;
			if (confirmExpectedRequests) {
				Appendables.appendUTF8(accumulators[idx], pipe.blobRing, pos, len, pipe.blobMask);						    				
				
				while (accumulators[idx].length() >= ServerCoordinator.expectedGet.length()) {
					
				   int c = startsWith(accumulators[idx],ServerCoordinator.expectedGet); 
				   if (c>0) {
					   
					   String remaining = accumulators[idx].substring(c*ServerCoordinator.expectedGet.length());
					   accumulators[idx].setLength(0);
					   accumulators[idx].append(remaining);							    					   
					   
					   
				   } else {
					   logger.info("A"+Arrays.toString(ServerCoordinator.expectedGet.getBytes()));
					   logger.info("B"+Arrays.toString(accumulators[idx].subSequence(0, ServerCoordinator.expectedGet.length()).toString().getBytes()   ));
					   
					   logger.info("FORCE EXIT ERROR at {} exlen {}",pos,ServerCoordinator.expectedGet.length());
					   System.out.println(accumulators[idx].subSequence(0, ServerCoordinator.expectedGet.length()).toString());
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
    
    
}
