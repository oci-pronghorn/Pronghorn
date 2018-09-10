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
import com.ociweb.pronghorn.util.SelectedKeyHashMapHolder;

/**
 * Client-side stage that reads sockets using a ClientCoordinator
 * based on a release acknowledgment.
 * Accepts only expected calls (unlike ServerSocketReaderStage), since
 * it is a client.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ClientSocketReaderStage extends PronghornStage {	
	
    //if this is turned off not client requests will ever time out, turn off at your own risk.
    public static boolean abandonSlowConnections = true;
    
	private static final int SIZE_OF_PLAIN = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210);
	private final ClientCoordinator coordinator;
	private final Pipe<NetPayloadSchema>[] output;
	private final Pipe<ReleaseSchema>[] releasePipes;
	private final static Logger logger = LoggerFactory.getLogger(ClientSocketReaderStage.class);

	public static boolean showResponse = false;
	
	private long start;
	private long totalBytes=0;

	private final static int KNOWN_BLOCK_ENDING = -1;
	private int iteration;
	
	private int rateMask;
	private final GraphManager graphManger;
	
	
	/**
	 *
	 * @param graphManager
	 * @param coordinator
	 * @param parseAck _in_ The release acknowledgment input pipes.
	 * @param output _out_ The read payload from the socket.
	 */
	public ClientSocketReaderStage(GraphManager graphManager,
			                       ClientCoordinator coordinator, 
			                       Pipe<ReleaseSchema>[] parseAck, 
			                       Pipe<NetPayloadSchema>[] output) {
		super(graphManager, parseAck, output);
		this.coordinator = coordinator;
		this.output = output;
		this.releasePipes = parseAck;
		
		coordinator.setStart(this);
		
		//this resolves the problem of detecting this loop by the scripted fixed scheduler.
		GraphManager.addNota(graphManager, GraphManager.PRODUCER, GraphManager.PRODUCER, this);
		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lavenderblush", this);
		GraphManager.addNota(graphManager, GraphManager.LOAD_BALANCER, GraphManager.LOAD_BALANCER, this);
		     
		this.graphManger = graphManager;
		
		//if the minimum timeout is below the default rate we must lower the rate for this one stage to capture the timeouts as needed
		Number defaultRate = (Number)GraphManager.getNota(graphManager, this, GraphManager.SCHEDULE_RATE, null);
		if (null!=defaultRate && ClientCoordinator.minimumTimeout()<defaultRate.longValue()) {
			GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, new Long(ClientCoordinator.minimumTimeout()), this);
		}
	}

	
	
	@Override
	public void startup() {

		selectedKeyHolder = new SelectedKeyHashMapHolder();
		start = System.currentTimeMillis();
		
        Number schedRate = ((Number)GraphManager.getNota(graphManger, this, GraphManager.SCHEDULE_RATE, new Long(-1)));        
        long minimumTimeout = ClientCoordinator.minimumTimeout();        
        
        if (minimumTimeout < Long.MAX_VALUE && minimumTimeout>=0) {
        	rateMask = (1 << (int)(Math.log((int)( minimumTimeout / schedRate.longValue() ))/Math.log(2)))-1;
        } else {
        	rateMask = 0xFFF;
        }
        
        
	}
	
	@Override
	public void shutdown() {
		long duration = System.currentTimeMillis()-start;
		if (duration>0) {
			logger.trace("Client Bytes Read: {} kb/sec {} ",totalBytes, (8*totalBytes)/duration);
		}
	}

	int maxWarningCount = 10;
	
	
	private boolean shutdownInProgress;
	private final ArrayList<SelectionKey> doneSelectors = new ArrayList<SelectionKey>(100);
    private final Consumer<SelectionKey> selectionKeyAction = new Consumer<SelectionKey>(){
			@Override
			public void accept(SelectionKey selection) {
				processSelection(selection); 
			}
    };
    
    private SelectedKeyHashMapHolder selectedKeyHolder;
	private final BiConsumer keyVisitor = new BiConsumer() {
		@Override
		public void accept(Object k, Object v) {
			selectionKeyAction.accept((SelectionKey)k);
		}
	};
	
	private Set<SelectionKey> selectedKeys;
	
	@Override
	public void run() { //TODO: this method is the new hot spot in the profiler.
       
	   	 if(!shutdownInProgress) {
	   		 consumeRelease();
	         ////////////////////////////////////////
	         ///Read from socket
	         ////////////////////////////////////////

	     	Selector selector = coordinator.selector();
    		if (!selector.keys().isEmpty()) {

	    		///////////////////
	    		//after this point we are always checking for new data work so always record this
	    		////////////////////
		     	if (null != this.didWorkMonitor) {
		     		this.didWorkMonitor.published();
		     	}	
		     	 
		     	 //max cycles before we take a break.
		     	int maxIterations = 100; //important or this stage will take all the resources.
		     	
		         while (--maxIterations>=0 & hasNewDataToRead(selector) ) { //single & to ensure we check has new data to read.
	
		 	           doneSelectors.clear();
		 		
		 	           hasRoomForMore = true;
		 	           
		 	           HashMap keyMap = selectedKeyHolder.selectedKeyMap(selectedKeys);
		 	           if (null!=keyMap) {
		 	        	   keyMap.forEach(keyVisitor);
		 	           } else {
		 	        	   selectedKeys.forEach(selectionKeyAction);
		 	           }
		 	           
		 			   removeDoneKeys(selectedKeys);
		 			      
		 			   if (!hasRoomForMore) {
		 				   break;
		 			   }
		 		
		         }
    		}
	  
	  		if (abandonSlowConnections && ((++iteration & rateMask)==0) ) {        	

	         	        	ClientAbandonConnectionScanner slowConnections = coordinator.scanForSlowConnections();
	 						ClientConnection abandonded = slowConnections.leadingCandidate();
	         	        	if (null!=abandonded) {
	         	        		abandonNow(abandonded);         	        		
	         	        	}
	         	        	ClientConnection[] timedOut = slowConnections.timedOutConnections();
	         	        	int i = timedOut.length;
	         	        	while (--i >= 0) {
	         	        		if (null != timedOut[i]) {
	         	        			abandonNow(timedOut[i]);
	         	        		}
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

	private void abandonNow(ClientConnection abandonded) {
		
		
		
		
		//we only close connections which are currently holding a pipe reservation open
		//never grab a new one here or it may cause a hang,  only close those already with reservation
		int pipeIdx;// = coordinator.checkForResponsePipeLineIdx(abandonded.getId());
		
		//test because we are not closing connections which hAVE NO PIPE FOR THE DISCONNECT...
		//if (pipeIdx<0) {
			pipeIdx = ClientCoordinator.responsePipeLineIdx(coordinator, abandonded.getId());
		//}
		
		
		
		if (pipeIdx>=0) {
			Pipe<NetPayloadSchema> pipe = output[pipeIdx];	        	        	
			///ensure that this will not cause any stall, better to skip this than be blocked.
			if (Pipe.hasRoomForWrite(pipe)) {
				
				long nowNS = System.nanoTime();
				long callTime = abandonded.outstandingCallTime(nowNS);
				logger.warn("\nClient disconnected {} con:{} session:{} because call was taking too long. Estimated:{}",
						 abandonded, abandonded.id, abandonded.sessionId,Appendables.appendNearestTimeUnit(new StringBuilder(), callTime));								

				abandonded.touchSentTime(nowNS);//rest the timeout so we do not attempt to close this again until the timeout has passed again.				
				
				if (!abandonded.isDisconnecting()) {
					abandonded.beginDisconnect();
				}
				
				int size = Pipe.addMsgIdx(pipe, NetPayloadSchema.MSG_DISCONNECT_203);
				Pipe.addLongValue(abandonded.getId(), pipe);
				Pipe.confirmLowLevelWrite(pipe, size);
				Pipe.publishWrites(pipe);    
								
				//only release after we populate the pipe.
				coordinator.releaseResponsePipeLineIdx(abandonded.getId());
				//Do not set notification sent this message will trigger that one later once it makes it down the pipe.
				
			}
		}
	}

	
	boolean hasRoomForMore = true;
	private void processSelection(SelectionKey selection) {
		assert isReadOpsOnly(selection) : "only expected read"; 
			
		//System.err.println("processSelection");
		
		ClientConnection cc = (ClientConnection)selection.attachment();

		boolean didWork = false;
		if (!cc.isClientClosedNotificationSent() && !cc.isDisconnecting()) {
			didWork = processConnection(didWork, cc); // if we need the channel we can get it from selection.channel()....
		} else {
			didWork = true;
		}
		//always remove in-case we need to get to the following
		doneSelectors.add(selection);
		if (!didWork) {
			hasRoomForMore = false;//if any one is blocked go work elsewhere.
		}

		
	}

	private boolean isReadOpsOnly(SelectionKey selection) {
		try {
			return 0 != (SelectionKey.OP_READ & selection.readyOps());
		} catch (Throwable t) {
			return true;//No exceptions should cause this check to fail
		}
	}
	
	long sum = 0;
	long sumc = 0;
	
	private void removeDoneKeys(Set<SelectionKey> selectedKeys) {
		//sad but this is the best way to remove these without allocating a new iterator
		// the selectedKeys.removeAll(doneSelectors); will produce garbage upon every call
		int c = doneSelectors.size();

		//logger.info("remove {} done selector keys out of {} ",c, selectedKeys.size());
		while (--c>=0) {
		    		selectedKeys.remove(doneSelectors.get(c));
		}
		
		
	}
	
    private boolean hasNewDataToRead(Selector selector) {
    	
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



	private boolean processConnection(boolean didWork, ClientConnection cc) {
		//process handshake before reserving one of the pipes
		boolean doRead = true;
		if (coordinator.isTLS) {
			
			 HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();
			 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
			
		            Runnable task;//TODO: there is an opporuntity to have this done by a different stage in the future.
		            while ((task = cc.getEngine().getDelegatedTask()) != null) {
		            	task.run();
		            }
			 } else if (HandshakeStatus.NEED_WRAP == handshakeStatus) {		
				 consumeRelease();
				 doRead = false;
				 //one of the other pipes can do work
			 }	    		 
			 
		}

		if (doRead) {
			
			//holds the pipe until we gather all the data and got the end of the parse.
			int pipeIdx = ClientCoordinator.responsePipeLineIdx(coordinator, cc.getId());//picks any open pipe to keep the system busy
			if (pipeIdx>=0) {
				assert(pipeIdx<output.length) : "Bad pipe idx of "+pipeIdx+" but we only have "+output.length+" pipes";
				didWork = readFromSocket(didWork, cc, output[pipeIdx]);
			} else {	    	
				consumeRelease();
				pipeIdx = ClientCoordinator.responsePipeLineIdx(coordinator, cc.getId()); //try again.
				if (pipeIdx>=0) {
					//was able to reserve a pipe run 
					didWork = readFromSocket(didWork, cc, output[pipeIdx]);
				}				    		
			}
			
		} else {
			didWork = false;
		}
		return didWork;
	}

	private boolean readFromSocket(boolean didWork, ClientConnection cc, Pipe<NetPayloadSchema> target) {
		
		if (Pipe.hasRoomForWrite(target) ) {			
	
			//these buffers are only big enough to accept 1 target.maxAvgVarLen
			ByteBuffer[] wrappedUnstructuredLayoutBufferOpen = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(target),target);

			int readCount=-1; 
			SocketChannel socketChannel = (SocketChannel)cc.getSocketChannel();
			if (null != socketChannel) {
				try {
					//NOTE: warning note cast to int.
					readCount = (int)socketChannel.read(wrappedUnstructuredLayoutBufferOpen);
				} catch (IOException ioex) {
					readCount = -1;
					//			logger.info("\nUnable to read socket, may not be an error. data was droped. ",ioex);
				}
			}
			
			if (readCount>0) {
				didWork = true;
				totalBytes += readCount;						    		
				//we read some data so send it
				
				if (!coordinator.isTLS) {
					writePlain(cc, target, readCount);
				} else {
					writeEncrypted(cc.getId(), target, readCount);
				}
			} else {
				//logger.info("zero read detected client side..");
				//nothing to send so let go of byte buffer.
				Pipe.unstoreBlobWorkingHeadPosition(target);
			}
		}
		return didWork;
	}

	private void writePlain(ClientConnection cc, Pipe<NetPayloadSchema> target, int readCount) {

		Pipe.presumeRoomForWrite(target);
		Pipe.addMsgIdx(target, NetPayloadSchema.MSG_PLAIN_210);
		Pipe.addLongValue(cc.getId(), target);         //connection
		Pipe.addLongValue(System.nanoTime(), target);
		Pipe.addLongValue(KNOWN_BLOCK_ENDING, target); //position
		
		int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(target);
		//NOTE: this is done manually to avoid the length validation check since we may do 2 messages worth.
		//blob head position is moved forward
		if ((int)readCount>0) { //len can be 0 so do nothing, len can be -1 for eof also nothing to move forward
			Pipe.addAndGetBlobWorkingHeadPosition(target, (int)readCount);
		}
		//record the new start and length to the slab for this blob
		Pipe.addBytePosAndLen(target, originalBlobPosition, (int)readCount);
		////////////////////////////////////////////////////////////
		
		if (showResponse) {
				logger.info("\n///ClientSocketReader////////\n"+
			   			Appendables.appendUTF8(new StringBuilder(), target.blobRing, originalBlobPosition, readCount, target.blobMask)+
			   			"\n//////////////////////");
		}
		
		Pipe.confirmLowLevelWrite(target, SIZE_OF_PLAIN);
		Pipe.publishWrites(target);

	}

	private void writeEncrypted(long id, Pipe<NetPayloadSchema> target, int readCount) {
		assert(Pipe.hasRoomForWrite(target)) : "checked earlier should not fail";
		
		int size = Pipe.addMsgIdx(target, NetPayloadSchema.MSG_ENCRYPTED_200);
		Pipe.addLongValue(id, target);
		Pipe.addLongValue(System.nanoTime(), target);
		
		int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(target);
		//NOTE: this is done manually to avoid the length validation check since we may do 2 messages worth.
		//blob head position is moved forward
		if ((int)readCount>0) { //len can be 0 so do nothing, len can be -1 for eof also nothing to move forward
			Pipe.addAndGetBlobWorkingHeadPosition(target, (int)readCount);
		}
		//record the new start and length to the slab for this blob
		Pipe.addBytePosAndLen(target, originalBlobPosition, (int)readCount);
		//////////////////////////////////////////////////////////
		
		Pipe.confirmLowLevelWrite(target, size);
		Pipe.publishWrites(target);

	}

	long lastTotalBytes = 0;

	
   //must be called often to keep empty.
	private boolean consumeRelease() {
		
		boolean didWork = false;
		int i = releasePipes.length;
		while (--i>=0) {			
			Pipe<ReleaseSchema> ack = releasePipes[i];
			
			while ((!Pipe.isEmpty(ack)) && Pipe.hasContentToRead(ack)) {
				
				didWork = true;
				
				int id = Pipe.takeMsgIdx(ack);
				if (id == ReleaseSchema.MSG_RELEASE_100) {
					
					consumeRelease(Pipe.takeLong(ack), Pipe.takeLong(ack));
	    			
	    			Pipe.confirmLowLevelRead(ack, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASE_100));
				} else if (id == ReleaseSchema.MSG_RELEASEWITHSEQ_101) {
					
					consumeRelease(Pipe.takeLong(ack), Pipe.takeLong(ack));
					int fieldSequenceNo = Pipe.takeInt(ack);
					
					
					Pipe.confirmLowLevelRead(ack, Pipe.sizeOf(ReleaseSchema.instance, ReleaseSchema.MSG_RELEASEWITHSEQ_101));
				}else {
					assert(-1 == id) : "unexpected id of "+id;
					Pipe.confirmLowLevelRead(ack, Pipe.EOF_SIZE);
				}
				Pipe.releaseReadLock(ack);
			}
			
		}
		return didWork;
	}

	public void consumeRelease(long fieldConnectionId, long fieldPosition) {
		///////////////////////////////////////////////////
		//if sent tail matches the current head then this pipe has nothing in flight and can be re-assigned
		int pipeIdx = coordinator.checkForResponsePipeLineIdx(fieldConnectionId);
		if (pipeIdx>=0 && Pipe.workingHeadPosition(output[pipeIdx]) == fieldPosition) {
			assert(Pipe.contentRemaining(output[pipeIdx])==0) : "unexpected content on pipe detected";
			assert(!Pipe.isInBlobFieldWrite(output[pipeIdx])) : "unexpected open blob field write detected";
			
			//every connection is locked down to a single input pipe until
			//the consumer "parser" finds a stopping point and can release the pipe for other usages.

			coordinator.releaseResponsePipeLineIdx(fieldConnectionId);

		}
	}

}
