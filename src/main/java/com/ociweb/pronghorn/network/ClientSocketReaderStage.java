package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
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

public class ClientSocketReaderStage extends PronghornStage {	
	
	private static final int SIZE_OF_PLAIN = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210);
	private final ClientCoordinator coordinator;
	private final Pipe<NetPayloadSchema>[] output;
	private final Pipe<ReleaseSchema>[] releasePipes;
	private final static Logger logger = LoggerFactory.getLogger(ClientSocketReaderStage.class);

	public static final boolean showResponse = false;
	
	private long start;
	private long totalBytes=0;

	private final static int KNOWN_BLOCK_ENDING = -1;

	
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
		//GraphManager.addNota(graphManager, GraphManager.SCHEDULE_RATE, 10000, this);
		 
	}
	
	@Override
	public void startup() {

		start = System.currentTimeMillis();
		
	}
	
	@Override
	public void shutdown() {
		long duration = System.currentTimeMillis()-start;
		if (duration>0) {
			logger.trace("Client Bytes Read: {} kb/sec {} ",totalBytes, (8*totalBytes)/duration);
		}
	}

	int maxWarningCount = 10;
	
	
	private int pendingSelections;

	private boolean shutdownInProgress;
	private final ArrayList<SelectionKey> doneSelectors = new ArrayList<SelectionKey>(100);
    private final Consumer<SelectionKey> selectionKeyAction = new Consumer<SelectionKey>(){
			@Override
			public void accept(SelectionKey selection) {
				processSelection(selection); 
			}
    };  
	
	
	@Override
	public void run() { //TODO: this method is the new hot spot in the profiler.

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
    	
    	Selector selector = coordinator.selector();
    	
    	consumeRelease();
    	
        while (--maxIterations>=0 & hasNewDataToRead(selector) ) { //single & to ensure we check has new data to read.

           Set<SelectionKey> selectedKeys = selector.selectedKeys();

           assert(selectedKeys.size()>0);	            
           
           doneSelectors.clear();
	
           selectedKeys.forEach(selectionKeyAction);
                 
		   removeDoneKeys(selectedKeys);
		      
        }
   	}

	private void processSelection(SelectionKey selection) {
		assert(0 != (SelectionKey.OP_READ & selection.readyOps())) : "only expected read"; 
		
		ClientConnection cc = (ClientConnection)selection.attachment();
		assert(cc.getSelectionKey() == selection);
		assert(cc.getSocketChannel() == (SocketChannel)selection.channel());
		
		boolean didWork = false;
		didWork = processConnection(didWork, cc);
		if (didWork) {
			pendingSelections--;
			doneSelectors.add(selection);
		}
		
	}
	
	long sum = 0;
	long sumc = 0;
	
	private void removeDoneKeys(Set<SelectionKey> selectedKeys) {
		//sad but this is the best way to remove these without allocating a new iterator
		// the selectedKeys.removeAll(doneSelectors); will produce garbage upon every call
		int c = doneSelectors.size();

//		if (c>0) {
//			long duration = System.nanoTime()- coordinator.sentTime;
//			sum+=duration;
//			sumc++;
//			Appendables.appendNearestTimeUnit(System.out, sum/sumc," avg\n");
//			//Appendables.appendNearestTimeUnit(System.out, duration," now\n");
//		}
		//logger.info("remove {} done selector keys out of {} ",c, selectedKeys.size());
		while (--c>=0) {
		    		selectedKeys.remove(doneSelectors.get(c));
		}
		
		
	}
	
    private boolean hasNewDataToRead(Selector selector) {
    	
    	if (pendingSelections>0) {
    		return true;
    	}
    		
        try {        	        	
        	////////////
        	//CAUTION - select now clears previous count and only returns the additional I/O operation counts which have become avail since the last time SelectNow was called
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
	

	private boolean processConnection(boolean didWork, ClientConnection cc) {
		//process handshake before reserving one of the pipes
		boolean doRead = true;
		if (coordinator.isTLS) {
			
			HandshakeStatus handshakeStatus = cc.getEngine().getHandshakeStatus();
			//if (cc.isValid()) {
			//	logger.info("has handshakeStatus data for {} {}",cc,handshakeStatus);
			//}
			
			 if (HandshakeStatus.NEED_TASK == handshakeStatus) {
			
		            Runnable task;//TODO: there is an opporuntity to have this done by a different stage in the future.
		            while ((task = cc.getEngine().getDelegatedTask()) != null) {
		            	task.run();
		            }
		            //TODO: delete we are done with task. or confrim the new status? as finished..
		            //handshakeStatus = cc.getEngine().getHandshakeStatus();
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
		if (Pipe.hasRoomForWrite(target)) {
	
			//these buffers are only big enough to accept 1 target.maxAvgVarLen
			ByteBuffer[] wrappedUnstructuredLayoutBufferOpen = Pipe.wrappedWritingBuffers(target);

			assert(target.maxVarLen+1 >= recvBufferSize(cc)) : 
				"The target buffer must be larger than the input buffer. "+
			    target.maxVarLen+" vs "+recvBufferSize(cc)+" vs "+coordinator.receiveBufferSize;
			
			//TODO: warning note cast to int.
			int readCount=-1; 
			try {					    			
				
				SocketChannel socketChannel = (SocketChannel)cc.getSocketChannel();
				readCount = (int)socketChannel.read(wrappedUnstructuredLayoutBufferOpen, 0, wrappedUnstructuredLayoutBufferOpen.length);
				
			} catch (IOException ioex) {
				readCount = -1;
				//logger.info("unable to read socket, may not be an error. ",ioex);
				//will continue with readCount of -1;
			}
   
			
			if (readCount>0) {
				didWork = true;
				totalBytes += readCount;						    		
				//we read some data so send it		
			
				logger.trace("totalbytes consumed by client {} TLS {} ",totalBytes, coordinator.isTLS);
				
				if (coordinator.isTLS) {
					assert(Pipe.hasRoomForWrite(target)) : "checked earlier should not fail";
					
					int size = Pipe.addMsgIdx(target, NetPayloadSchema.MSG_ENCRYPTED_200);
					Pipe.addLongValue(cc.getId(), target);
					Pipe.addLongValue(System.nanoTime(), target);
					
					int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(target);
					Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int)readCount, target);
					
					Pipe.confirmLowLevelWrite(target, size);
					Pipe.publishWrites(target);
												    		
				} else {
					assert(Pipe.hasRoomForWrite(target)) : "checked earlier should not fail";
					
					Pipe.addMsgIdx(target, NetPayloadSchema.MSG_PLAIN_210);
					Pipe.addLongValue(cc.getId(), target);         //connection
					Pipe.addLongValue(System.nanoTime(), target);
					Pipe.addLongValue(KNOWN_BLOCK_ENDING, target); //position
					
					int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(target);
					Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int)readCount, target);
		 
					
					if (showResponse) {
								    System.err.println("//////////////////////");
						   			Appendables.appendUTF8(System.err, target.blobRing, originalBlobPosition, readCount, target.blobMask);
						   			System.err.println("//////////////////////");
					}

					
					Pipe.confirmLowLevelWrite(target, SIZE_OF_PLAIN);
					Pipe.publishWrites(target);
													    			
				}						    		
				
			} else {
				//logger.info("zero read detected client side..");
				//nothing to send so let go of byte buffer.
				Pipe.unstoreBlobWorkingHeadPosition(target);
			}
		}
		return didWork;
	}

	private int recvBufferSize(ClientConnection cc) {
		
		try {
			return cc.getSocketChannel().getOption(StandardSocketOptions.SO_RCVBUF);
		} catch (Throwable e) {
			return -1;
		}
		
	}



	long lastTotalBytes = 0;

	
   //must be called often to keep empty.
	private boolean consumeRelease() {
		
		boolean didWork = false;
		int i = releasePipes.length;
		while (--i>=0) {			
			Pipe<ReleaseSchema> ack = releasePipes[i];
			
			while (Pipe.hasContentToRead(ack)) {
				
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
