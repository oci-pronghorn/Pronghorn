package com.ociweb.pronghorn.network;

import java.util.Arrays;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;


//consumes the sequence number in order and hold a pool entry for this connection
//sends the data in order to the right pool entry for encryption to be applied down stream.
//TODO: should add feature of subscriptions here due to it being before the encryption stage.
public class OrderSupervisorStage extends PronghornStage { //AKA re-ordering stage
    
    private static final int SIZE_OF_TO_CHNL = Pipe.sizeOf(ServerResponseSchema.instance, ServerResponseSchema.MSG_TOCHANNEL_100);
    private static final byte[] EMPTY = new byte[0];
    
	private static Logger logger = LoggerFactory.getLogger(OrderSupervisorStage.class);

    private final Pipe<ServerResponseSchema>[] dataToSend;
    private final Pipe<NetPayloadSchema>[] outgoingPipes;
        
        
    private int[]          expectedSquenceNos;

    private final ServerCoordinator coordinator;
    
    public final static int UPGRADE_TARGET_PIPE_MASK     = (1<<21)-1;
 
    public final static int UPGRADE_CONNECTION_SHIFT     = 31;    
    public final static int UPGRADE_MASK                 = 1<<UPGRADE_CONNECTION_SHIFT;
    
    public final static int CLOSE_CONNECTION_SHIFT       = 30;
    public final static int CLOSE_CONNECTION_MASK        = 1<<CLOSE_CONNECTION_SHIFT;
    
    public final static int END_RESPONSE_SHIFT           = 29;//for multi message send this high bit marks the end
    public final static int END_RESPONSE_MASK            = 1<<END_RESPONSE_SHIFT;
    
    public final static int INCOMPLETE_RESPONSE_SHIFT    = 28;
    public final static int INCOMPLETE_RESPONSE_MASK     = 1<<INCOMPLETE_RESPONSE_SHIFT;
    
//    public int[] lastLenWritten;
//    public long[] lastXXXWritten;
//    public long[] lastYYYWritten;
    
    public final int poolMod;
    public final int maxOuputSize;
    public final int plainSize = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210);
    private int shutdownCount;

    private final boolean isTLS;
	private final int groupId = 0; //TODO: pass in on construction, must know group to look up SSL connection.

    private StringBuilder[] accumulators; //for testing only
    
    
    /**
     * 
     * Data arrives from random input pipes, but each message has a channel id and squence id.
     * Data is ordered by squence number and sent to the pipe from the pool belonging to that specific channel id
     * 
     * 
     * @param graphManager
     * @param inputPipes
     * @param coordinator
     */
    public OrderSupervisorStage(GraphManager graphManager, Pipe<ServerResponseSchema>[][][] inputPipes, Pipe<NetPayloadSchema>[] outgoingPipes, ServerCoordinator coordinator, boolean isTLS) {
        super(graphManager, join(inputPipes), outgoingPipes);      
        this.dataToSend = join(inputPipes);
        
        this.outgoingPipes = outgoingPipes;
        this.coordinator = coordinator;
        this.isTLS = isTLS;
        this.poolMod = outgoingPipes.length;
        this.shutdownCount = dataToSend.length;
        
        this.supportsBatchedPublish = false;
        this.supportsBatchedRelease = false;
        
        if (minVarLength(outgoingPipes) < maxVarLength(this.dataToSend)) {
        	throw new UnsupportedOperationException("All output pipes must support variable length fields equal to or larger than all input pipes");
        }
        
        this.maxOuputSize = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210) +
        								Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_UPGRADE_307) +            
        								Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_DISCONNECT_203);
    }


	@Override
    public void startup() {                
        expectedSquenceNos = new int[coordinator.channelBitsSize];//room for 1 per active channel connection
        
        
        if (ServerCoordinator.TEST_RECORDS) {
			int i = outgoingPipes.length;
			accumulators = new StringBuilder[i];
			while (--i >= 0) {
				accumulators[i]=new StringBuilder();					
			}
        }
        
    }
	
	@Override
	public void shutdown() {

		int i = outgoingPipes.length;
		while (--i>=0) {
			Pipe.spinBlockForRoom(outgoingPipes[i], Pipe.EOF_SIZE);  //TODO: this is a re-occuring pattern perhaps this belongs in the base class since every actor does it.
			Pipe.publishEOF(outgoingPipes[i]);
		}
	}
    
//	int xa = 0;
//	int xb = 0;
//	int xc = 0;
//	long nextTime = 0;
	
	
	@Override
    public void run() {

		
		
    	boolean didWork;
    	do {
	    	didWork = false;
	        int c = dataToSend.length;
	        
	        
//	        long now = System.currentTimeMillis();
//	        if (now>nextTime) {	        	
//	        	logger.info("A:{} B:{} C:{}",xa,xb,xc);
//	        	nextTime = now + 500;
//	        }
	        
	        while (--c >= 0) {
	        	final Pipe<ServerResponseSchema> sourcePipe = dataToSend[c];
	        	
	        	//WARNING a single response sits on the pipe and its output is full so nothing happens until it is cleared.
	        	//System.err.println("process pipe: "+c+" of "+dataToSend.length);
	        	
	        	//Hold position pending, and write as data becomes available
	        	//this lets us read deep into the pipe
	        	
	        	//OR
	        	
	        	//app/module must write to different pipes to balance the load, 
	        	//this may happen due to complex logic but do we want to force this on all module makers?
	        	
	        	//NOTE: this is only a problem because the router takes all the messages from 1 connection before doing the next.
	        	//      if we ensure it is balanced then this data will also get balanced.
	        		        
	        	
	            didWork = processPipe(didWork, sourcePipe);
	            
            	
	            
	        }  
    	} while (didWork);
    }


	private boolean processPipe(boolean didWork, final Pipe<ServerResponseSchema> sourcePipe) {
		
		while (Pipe.hasContentToRead(sourcePipe)) {

		    //peek to see if the next message should be blocked, eg out of order, if so skip to the next pipe
		    int peekMsgId = Pipe.peekInt(sourcePipe, 0);
		    Pipe<NetPayloadSchema> myPipe = null;
		    int myPipeIdx = -1;
		    int sequenceNo = 0;
		    long channelId = -2;
		    if (peekMsgId>=0 && ServerResponseSchema.MSG_SKIP_300!=peekMsgId) {
		    	didWork = true;
		    	
		        channelId = Pipe.peekLong(sourcePipe, 1);
		        
		        //TODO: can we make the OrderSuper combine writes to avoid these pipe getting so full. they spike to 100% at times.
		        		
		        myPipeIdx = (int)(channelId % poolMod);
		        myPipe = outgoingPipes[myPipeIdx];
		        
		        ///////////////////////////////
		        //quit early if the pipe is full, NOTE: the order super REQ long output pipes
		        ///////////////////////////////
		    	if (!Pipe.hasRoomForWrite(myPipe, maxOuputSize)) {
		   // 		xb++;
		    		break;
		    	}
		    	
		    	
		        sequenceNo = Pipe.peekInt(sourcePipe,  3);	                   
		    
		        //read the next non-blocked pipe, sequenceNo is never reset to zero
		        //every number is used even if there is an exception upon write.
		        
		     //   System.err.println("channel ID mask "+Integer.toHexString(coordinator.channelBitsMask));
		        
		        int expected = expectedSquenceNos[(int)(channelId & coordinator.channelBitsMask)];                
		        if (sequenceNo!=expected) {          
		        	assert(sequenceNo>=expected) : "found smaller than expected sequenceNo, they should never roll back";	 
		        	break;
		        }	                  

		        if (isTLS) {
		            handshakeProcessing(myPipe, channelId);	                    
		        }
		         
		        
		    } else {
		    	didWork = true;
		    	

			    
		    	////////////////
		    	//these consume data but do not write out to pipes
		    	////////////////
		    	int idx = Pipe.takeMsgIdx(sourcePipe);
		    	
		    	if (ServerResponseSchema.MSG_SKIP_300 ==idx) {

		    		skipDataBlock(sourcePipe, idx);
		        	
		    		continue;
		    	} else {	
		        	Pipe.confirmLowLevelRead(sourcePipe, Pipe.EOF_SIZE);
		        	Pipe.releaseReadLock(sourcePipe);
		        	
		        	if (--shutdownCount<=0) {
		        		requestShutdown();
		        		break;
		        	} else {
		        		continue;
		        	}
		    	}
		    }
		    
		    copyDataBlock(sourcePipe, peekMsgId, myPipe, myPipeIdx, sequenceNo, channelId);
		}
		return didWork;
	}


	private void copyDataBlock(final Pipe<ServerResponseSchema> sourcePipe, int peekMsgId,
			Pipe<NetPayloadSchema> myPipe, int myPipeIdx, int sequenceNo, long channelId) {
		////////////////////////////////////////////////////
		//we now know that this work should be done and that there is room to put it out on the pipe
		//so do it already
		////////////////////////////////////////////////////
		//the EOF message has already been taken so no need to check 
		//all remaning messages start with the connection id
		{
		    long value = channelId;
		    final int activeMessageId = Pipe.takeMsgIdx(sourcePipe);
		              	                
		    assert(peekMsgId == activeMessageId);
		    final long oldChannelId = channelId;
		    
		    long channelId2 = Pipe.takeLong(sourcePipe);
		    
		    assert(oldChannelId == channelId2) : ("channel mismatch "+oldChannelId+" "+channelId2);
		    
			
			
   //TOOD: can we combine multiple message into the same  block going out to the same destination????
			
			
		    //most common case by far so we put it first
		    if (ServerResponseSchema.MSG_TOCHANNEL_100 == activeMessageId ) {
		    	                	             	  
		    	 publishDataBlock(sourcePipe, myPipe, myPipeIdx, sequenceNo, channelId2);
		    	
		    } else {
		    	
		    	throw new UnsupportedOperationException("not yet implemented "+activeMessageId);
		    	
		    } 
		}
	}


	private void skipDataBlock(final Pipe<ServerResponseSchema> sourcePipe, int idx) {
		//logger.info("processing skip");
		//logger.info("debug: tail "+mTail+" head  "+mHead+" readBase "+readBase);
		
		
		int meta = Pipe.takeRingByteMetaData(sourcePipe);
		int len = Pipe.takeRingByteLen(sourcePipe);
		Pipe.bytePosition(meta, sourcePipe, len); //this does the skipping
		
		//logger.info(" new base "+Pipe.bytesReadBase(sourcePipe)+" skipped bytes "+len);
		
		Pipe.confirmLowLevelRead(sourcePipe,Pipe.sizeOf(ServerResponseSchema.instance, idx));
		Pipe.releaseReadLock(sourcePipe);
	}


	private void publishDataBlock(final Pipe<ServerResponseSchema> sourcePipe, Pipe<NetPayloadSchema> myPipe,
			int myPipeIdx, int sequenceNo, long channelId) {
		int expSeq = Pipe.takeInt(sourcePipe); //sequence number
		 assert(sequenceNo == expSeq);
		 
		 //byteVector is payload
		 int meta = Pipe.takeRingByteMetaData(sourcePipe); //for string and byte array
		 int len = Pipe.takeRingByteLen(sourcePipe);
		
		         
		 int requestContext = Pipe.takeInt(sourcePipe); //high 1 upgrade, 1 close low 20 target pipe	                     
		 int blobMask = Pipe.blobMask(sourcePipe);
		 byte[] blob = Pipe.byteBackingArray(meta, sourcePipe);
		 int bytePosition = Pipe.bytePosition(meta, sourcePipe, len);
		 
		 //view in the console what we just wrote out to the next stage.
		 //System.out.println("id "+myPipeIdx);
		 //Appendables.appendUTF8(System.out, blob, bytePosition, len, blobMask);
		 
		 if (ServerCoordinator.TEST_RECORDS) {
			 //check 
			 testValidContent(myPipeIdx, sourcePipe, meta, len);
			 
			 //testValidContentQuick(sourcePipe, meta, len);
		 }
		 
		 Pipe.confirmLowLevelRead(sourcePipe, SIZE_OF_TO_CHNL);	                     
		 Pipe.readNextWithoutReleasingReadLock(sourcePipe);//we will look ahead to see what we can combine into a single read
		 
		 
		 //TODO: peek here to see if making a larger block is even possible, because we have skip gap this may not work out very well.
		 //TODO: this copy is the hot point in the code because we are calling it too many times with SMALL copies
		 //TODO: it would be nice to roll up multiple writes if possible to minimize overhead KEY UPGRADE HERE.
		 
		 //TODO: we need the oder super to combine the parts into 1 write if possible.
		 //TODO: skip over any skip and do 2 copies to a single target.
		 
		 //we can just bump up len OR do 2 blocks?
		 

		 
		 
		 //If a response was sent as muliple parts all part of the same sequence number then we roll them up as a single write when possible.
		 while (Pipe.peekMsg(sourcePipe, ServerResponseSchema.MSG_TOCHANNEL_100) 
				 && Pipe.peekLong(sourcePipe, 1) == channelId 
			 	 && Pipe.peekInt(sourcePipe, 3)==expSeq 
			     && (len+Pipe.peekInt(sourcePipe, 5)<myPipe.maxAvgVarLen)  ) {
					 //this is still part of the current response so combine them together
					 
					 Pipe.takeMsgIdx(sourcePipe);
					 Pipe.takeLong(sourcePipe);
					 Pipe.takeInt(sourcePipe);
					 
					 int meta2 = Pipe.takeRingByteMetaData(sourcePipe); //for string and byte array
					 int len2 = Pipe.takeRingByteLen(sourcePipe);
					 
					 int pos2 = Pipe.bytePosition(meta2, sourcePipe, len2); //moves pointer
	 
					 len+=len2;
					 
					 requestContext = Pipe.takeInt(sourcePipe); //this replaces the previous context read
		
					 Pipe.confirmLowLevelRead(sourcePipe, SIZE_OF_TO_CHNL);	 
					 Pipe.readNextWithoutReleasingReadLock(sourcePipe);
					 
		 }
		 
		 
		 writeToNextStage(myPipe, channelId, len, requestContext, blobMask, blob, bytePosition); 
		 
		 //TODO: we should also look at the module definition logic so we can have mutiple OrderSuper instances (this is the first solution).
		 
		 Pipe.releaseAllPendingReadLock(sourcePipe); //now done consuming the bytes so release the pending read lock release.
		 
		 
		 //Pipe.releaseReadLock(sourcePipe);
	}


	private void testValidContentQuick(final Pipe<ServerResponseSchema> sourcePipe, int meta, int len) {
		int pos = Pipe.convertToPosition(meta, sourcePipe);
		 
		 char ch = (char)sourcePipe.blobRing[sourcePipe.blobMask&pos];
		 if (ch=='H') {
			 if (89!=len) {
				 logger.info("ERROR FORCE EXIT B {}",len);
				 System.exit(-1);
			 }
			// System.err.println("h "+len);
			 
		 } else if (ch=='{') {			 
			 if (30!=len) {
				 logger.info("ERROR FORCE EXIT A {}",len);
				 System.exit(-1);
			 }
 			// System.err.println("{ "+len);
			 
		 } else  {
			 System.err.println(ch);
			 logger.info("ERROR FORCE EXIT odd unexpected char in incomming pipe");
			 System.exit(-1);
		 }
		
		 char ch2 = (char)sourcePipe.blobRing[sourcePipe.blobMask&(pos+len-1)];
		 if (ch2!=10) {
			 System.err.println(ch2);
			 logger.info("ERROR FORCE EXIT2");
			 System.exit(-1);
		 }
	}
	
	private void testValidContent(final int idx, Pipe<?> pipe, int meta, int len) {
		
		if (ServerCoordinator.TEST_RECORDS) {
			
			//write pipeIdx identifier.
			//Appendables.appendUTF8(System.out, target.blobRing, originalBlobPosition, readCount, target.blobMask);
			
			int pos = Pipe.convertToPosition(meta, pipe);
			    				
			
			boolean confirmExpectedRequests = true;
			if (confirmExpectedRequests) {
				Appendables.appendUTF8(accumulators[idx], pipe.blobRing, pos, len, pipe.blobMask);						    				
				
				while (accumulators[idx].length() >= ServerCoordinator.expectedOK.length()) {
					
				   int c = startsWith(accumulators[idx],ServerCoordinator.expectedOK); 
				   if (c>0) {
					   
					   String remaining = accumulators[idx].substring(c*ServerCoordinator.expectedOK.length());
					   accumulators[idx].setLength(0);
					   accumulators[idx].append(remaining);							    					   
					   
					   
				   } else {
					   logger.info("A"+Arrays.toString(ServerCoordinator.expectedOK.getBytes()));
					   logger.info("B"+Arrays.toString(accumulators[idx].subSequence(0, ServerCoordinator.expectedOK.length()).toString().getBytes()   ));
					   
					   logger.info("FORCE EXIT ERROR at {} exlen {}",pos, ServerCoordinator.expectedOK.length());
					   System.out.println(accumulators[idx].subSequence(0, ServerCoordinator.expectedOK.length()).toString());
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


	private void handshakeProcessing(Pipe<NetPayloadSchema> pipe, long channelId) {
		SSLConnection con = coordinator.get(channelId, groupId);
		
		HandshakeStatus hanshakeStatus = con.getEngine().getHandshakeStatus();
		do {
		    if (HandshakeStatus.NEED_TASK == hanshakeStatus) {
		         Runnable task;
		         while ((task = con.getEngine().getDelegatedTask()) != null) {
		            	task.run(); 
		         }
		       hanshakeStatus = con.getEngine().getHandshakeStatus();
		    } 
		    
		    if (HandshakeStatus.NEED_WRAP == hanshakeStatus) {
		    	if (Pipe.hasRoomForWrite(pipe)) {
		    		int size = Pipe.addMsgIdx(pipe, NetPayloadSchema.MSG_PLAIN_210);
		    		Pipe.addLongValue(con.getId(), pipe);//connection
		    		Pipe.addLongValue(System.currentTimeMillis(), pipe);
		    		Pipe.addLongValue(SSLUtil.HANDSHAKE_POS, pipe); //signal that WRAP is needed 
		    		Pipe.addByteArray(EMPTY, 0, 0, pipe);
		    		Pipe.confirmLowLevelWrite(pipe, size);
		    		Pipe.publishWrites(pipe);
		    		
		    	} else {
					//no room to request wrap, try later
		        	break;
				}
		    } 
		} while ((HandshakeStatus.NEED_TASK == hanshakeStatus) || (HandshakeStatus.NEED_WRAP == hanshakeStatus));
		assert(HandshakeStatus.NEED_UNWRAP != hanshakeStatus) : "Unexpected unwrap request";
	}


	private void writeToNextStage(Pipe<NetPayloadSchema> myPipe, final long channelId, int len, int requestContext,
			int blobMask, byte[] blob, int bytePosition) {
		/////////////
		 //if needed write out the upgrade message
		 ////////////
		 
		 if (0 != (UPGRADE_MASK & requestContext)) {
			 
			 //the next response should be routed to this new location
			 int upgSize = Pipe.addMsgIdx(myPipe, NetPayloadSchema.MSG_UPGRADE_307);
			 Pipe.addLongValue(channelId, myPipe);
			 Pipe.addIntValue(UPGRADE_TARGET_PIPE_MASK & requestContext, myPipe);;
			 Pipe.confirmLowLevelWrite(myPipe, upgSize);
			 Pipe.publishWrites(myPipe);
			 
		 }
		                	
		 /////////////
		 //write out the content
		 /////////////
		 
		 
		 //logger.info("write content from super to wrapper sizse {} for {}",len,channelId);
		 
		 int plainSize = Pipe.addMsgIdx(myPipe, NetPayloadSchema.MSG_PLAIN_210);
		 Pipe.addLongValue(channelId, myPipe);
		 Pipe.addLongValue(System.currentTimeMillis(), myPipe);
		 Pipe.addLongValue(Pipe.getWorkingTailPosition(myPipe), myPipe);
		 Pipe.addByteArrayWithMask(myPipe, blobMask, len, blob, bytePosition);
		 
		 Pipe.confirmLowLevelWrite(myPipe, plainSize);
		 Pipe.publishWrites(myPipe);
		 
		 if (0 != (END_RESPONSE_MASK & requestContext)) {
		    //we have finished all the chunks for this request so the sequence number will now go up by one	
		 	expectedSquenceNos[(int)(channelId & coordinator.channelBitsMask)]++;
		 	
		 //	logger.info("increment expected for chnl {}  to value {} ",channelId, expectedSquenceNos[(int)(channelId & coordinator.channelBitsMask)]);
		 	
		 } 
		                     
		 
		 //////////////
		 //if needed write out the close connection message
		 //////////////
		 
		 if (0 != (CLOSE_CONNECTION_MASK & requestContext)) { 
			 
			 //logger.info("CLOSE CONNECTION DETECTED IN WRAP SUPER");
		     
			 int disSize = Pipe.addMsgIdx(myPipe, NetPayloadSchema.MSG_DISCONNECT_203);
			 Pipe.addLongValue(channelId, myPipe);
			 Pipe.confirmLowLevelWrite(myPipe, disSize);
			 Pipe.publishWrites(myPipe);
			 
		 }
	}
    
}
