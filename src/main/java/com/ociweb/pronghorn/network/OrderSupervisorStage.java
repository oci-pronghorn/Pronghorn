package com.ociweb.pronghorn.network;

import java.util.Arrays;

import javax.net.ssl.SSLEngineResult.HandshakeStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
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
        
        
    private int[]            expectedSquenceNos;
    private short[]          expectedSquenceNosPipeIdx;
    private long[]           expectedSquenceNosChannelId;

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
    
    public final int poolMod;
    public final int maxOuputSize;
    public final int plainSize = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210);
    private int shutdownCount;
    private boolean shutdownInProgress;

    private StringBuilder[] accumulators; //for testing only

    public static OrderSupervisorStage newInstance(GraphManager graphManager, Pipe<ServerResponseSchema>[] inputPipes, Pipe<NetPayloadSchema>[] outgoingPipes, ServerCoordinator coordinator, boolean isTLS) {
    	return new OrderSupervisorStage(graphManager, inputPipes, outgoingPipes, coordinator);
    }
    
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
    public OrderSupervisorStage(GraphManager graphManager, Pipe<ServerResponseSchema>[][] inputPipes, Pipe<NetPayloadSchema>[] outgoingPipes, ServerCoordinator coordinator, boolean isTLS) {
    	this(graphManager,join(inputPipes), outgoingPipes, coordinator);
    }
    
    public OrderSupervisorStage(GraphManager graphManager, Pipe<ServerResponseSchema>[] inputPipes, Pipe<NetPayloadSchema>[] outgoingPipes, ServerCoordinator coordinator) {
        super(graphManager, inputPipes, outgoingPipes);      
        this.dataToSend = inputPipes;
        assert(outgoingPipes.length>0);
        
        assert(dataToSend.length<=Short.MAX_VALUE) : "can not support more pipes at this time. This code will need to be modified";
        if (dataToSend.length>Short.MAX_VALUE) {
        	throw new UnsupportedOperationException("can not support more pipes at this time. This code will need to be modified");
        }
        
        this.outgoingPipes = outgoingPipes;
        this.coordinator = coordinator;

        this.poolMod = outgoingPipes.length;
        this.shutdownCount = dataToSend.length;
        
        this.supportsBatchedPublish = false;
        this.supportsBatchedRelease = false;
        
        if (minVarLength(outgoingPipes) < maxVarLength(inputPipes)) {
          	throw new UnsupportedOperationException(
        			"All output<NetPayloadSchema> pipes must support variable length fields equal to or larger"
        			+ " than all input<ServerResponseSchema> pipes. out "+minVarLength(outgoingPipes)+" in "+maxVarLength(inputPipes));
        }
        
        this.maxOuputSize = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210) +
        								Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_UPGRADE_307) +            
        								Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_DISCONNECT_203);
    }


	@Override
    public void startup() {                
		int totalChannels = coordinator.channelBitsSize; //WARNING: this can be large eg 4 million
        expectedSquenceNos = new int[totalChannels];//room for 1 per active channel connection
        
        expectedSquenceNosPipeIdx = new short[totalChannels];
        Arrays.fill(expectedSquenceNosPipeIdx, (short)-1);

        expectedSquenceNosChannelId = new long[totalChannels];
        Arrays.fill(expectedSquenceNosChannelId, (long)-1);
        
        //TODO: if we keep the long ChannelID we will know when the expectedSquenceNos should be zero again.
        //      these arrays are keeping one entry for each active connection.
        
        
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
			if (null!=outgoingPipes[i] && Pipe.isInit(outgoingPipes[i])) {
				Pipe.publishEOF(outgoingPipes[i]);
			}
		}
	}

	
	@Override
    public void run() {

		if (shutdownInProgress) {
			int i = outgoingPipes.length;
			while (--i>=0) {
				if (null!=outgoingPipes[i] && Pipe.isInit(outgoingPipes[i])) {
					if (!Pipe.hasRoomForWrite(outgoingPipes[i], Pipe.EOF_SIZE)) {
						return;
					}
				}
			}
			requestShutdown();
			return;
		}
		
    	boolean haveWork;
    	int maxIterations = 100;
    	do {
	    	haveWork = false;
	        int c = dataToSend.length;

	        
	        while (--c >= 0) {
	        	
	        	
	        	//WARNING a single response sits on the pipe and its output is full so nothing happens until it is cleared.
	        	//System.err.println("process pipe: "+c+" of "+dataToSend.length);
	        	
	        	//Hold position pending, and write as data becomes available
	        	//this lets us read deep into the pipe
	        	
	        	//OR
	        	
	        	//app/module must write to different pipes to balance the load, 
	        	//this may happen due to complex logic but do we want to force this on all module makers?
	        	
	        	//NOTE: this is only a problem because the router takes all the messages from 1 connection before doing the next.
	        	//      if we ensure it is balanced then this data will also get balanced.
	        		        
	        	
	            haveWork |= processPipe(dataToSend[c], c);
	            
            	
	            
	        }  
    	} while (--maxIterations>0 && haveWork);
    }


	private boolean processPipe(final Pipe<ServerResponseSchema> sourcePipe, int pipeIdx) {
		
		boolean didWork = false;
		while (Pipe.hasContentToRead(sourcePipe)) {
	
			assert(Pipe.bytesReadBase(sourcePipe)>=0);
						
			didWork = true;

		    //peek to see if the next message should be blocked, eg out of order, if so skip to the next pipe
		    int peekMsgId = Pipe.peekInt(sourcePipe, 0);
		    Pipe<NetPayloadSchema> myPipe = null;
		    int myPipeIdx = -1;
		    int sequenceNo = 0;
		    long channelId = -2;
		    if (peekMsgId>=0 && ServerResponseSchema.MSG_SKIP_300!=peekMsgId) {
		    			    	
		    	
		        channelId = Pipe.peekLong(sourcePipe, 1);		        
		        myPipeIdx = (int)(channelId % poolMod);
		        myPipe = outgoingPipes[myPipeIdx];
		        
		        //logger.trace("sending new content out for channelId {} ", channelId);
		        
		        ///////////////////////////////
		        //quit early if the pipe is full, NOTE: the order super REQ long output pipes
		        ///////////////////////////////
		    	if (!Pipe.hasRoomForWrite(myPipe, maxOuputSize)) {	
		    		assert(Pipe.bytesReadBase(sourcePipe)>=0);
		    		//logger.trace("no room to write out, try again later");
		    		break;
		    	}		    	
		    	
		        sequenceNo = Pipe.peekInt(sourcePipe,  3);	                   
		    
		        //read the next non-blocked pipe, sequenceNo is never reset to zero
		        //every number is used even if there is an exception upon write.
		      
		        int idx = (int)(channelId & coordinator.channelBitsMask);
		        
		        /////////////////////
		        //clear when we discover a new connection
		        ///////////////////
		        if (expectedSquenceNosChannelId[idx] != channelId) {
		        	expectedSquenceNos[idx] = 0;
		        	expectedSquenceNosChannelId[idx] = channelId;
		        	
					
			//		ServerCoordinator.newConnectionCounterStart = System.nanoTime();
					
		        }
		        
		        
				int expected = expectedSquenceNos[idx]; 
				
		        if (sequenceNo < expected) {
		        	
		        	assert(0==sequenceNo) : "Internal error request:"+sequenceNo+" expected:"+expected;
		        		        			     			        	
		            //this case happens upon disconnect / reconnect
		        	//	        	
		        	//sequenceNo 0 from request, with valid channelId x
		        	//expected Z shows that we already got a call on channelId x
		        	//if x got disconnected and re-used the expected will be up by one.
		        	//
		        	
		        	logger.trace("WARNING: older response {} expected {}",
		        			     sequenceNo, 
		        			     expected);
		        	
		        	//moved up sequence number and continue
		        	//rare case but we do not want to fail when it happens
		        	expectedSquenceNos[idx] =  expected = sequenceNo;		        	
	        	
		        	
		        } 
		        
		        if (expected==sequenceNo) {
		        	logger.trace("found expected sequence {}",sequenceNo);
		        	if (-1 == expectedSquenceNosPipeIdx[idx]) {
		        		expectedSquenceNosPipeIdx[idx]=(short)pipeIdx;
		        	} else {
		        		if (expectedSquenceNosPipeIdx[idx] !=(short)pipeIdx) {
		        			//drop the data
		        			logger.info("skipped older response B Pipe:{} vs Pipe:{} ",expectedSquenceNosPipeIdx[idx],pipeIdx);
		        			Pipe.skipNextFragment(sourcePipe);
				        	continue;
		        		}
		        	}
		        } else {
		            //larger value and not ready yet
		        	assert(sequenceNo>expected) : "found smaller than expected sequenceNo, they should never roll back";
		        	assert(Pipe.bytesReadBase(sourcePipe)>=0);
		        	logger.trace("not ready for sequence number yet, looking for {}  but found {}",expected,sequenceNo);
		        	//TODO: need to check 404 values and get this value
		        	
		        	continue;
//DELETE...		        	
//		        	//for not found 404 we will get these values, TODO: need a better approach 
//		        	expectedSquenceNos[idx] = sequenceNo;
//		        	break;//does not match
		        }
		        

		        if (coordinator.isTLS) {
		            handshakeProcessing(myPipe, channelId);	                    
		        }
		         
		        assert(Pipe.bytesReadBase(sourcePipe)>=0);
		        
		    } else {
			    
		    	////////////////
		    	//these consume data but do not write out to pipes
		    	////////////////
		    	int idx = Pipe.takeMsgIdx(sourcePipe);
		    	
		    	if (ServerResponseSchema.MSG_SKIP_300 ==idx) {
		    		assert(Pipe.bytesReadBase(sourcePipe)>=0);
		    		skipDataBlock(sourcePipe, idx);
		    		assert(Pipe.bytesReadBase(sourcePipe)>=0);
		    		logger.info("found skip");
		    		continue;
		    	} else {	
		        	Pipe.confirmLowLevelRead(sourcePipe, Pipe.EOF_SIZE);
		        	Pipe.releaseReadLock(sourcePipe);
		        	
		        	if (--shutdownCount<=0) {
		        		shutdownInProgress = true;
		        		assert(Pipe.bytesReadBase(sourcePipe)>=0);
		        		break;
		        	} else {
		        		//logger.trace("dec shutdown count");
		        		assert(Pipe.bytesReadBase(sourcePipe)>=0);
		        		continue;
		        	}
		    	}
		    }
		    assert(Pipe.bytesReadBase(sourcePipe)>=0);
			
		    copyDataBlock(sourcePipe, peekMsgId, myPipe, myPipeIdx, sequenceNo, channelId);
			
			
		    assert(Pipe.bytesReadBase(sourcePipe)>=0);
		}

		return didWork;
	}


	private void copyDataBlock(final Pipe<ServerResponseSchema> input, int peekMsgId,
							   final Pipe<NetPayloadSchema> output, int myPipeIdx,
							   int sequenceNo, long channelId) {
		 

    	//System.err.println("ttttttttttttt copy data block ");
    	
		assert(Pipe.bytesReadBase(input)>=0);
		 
		////////////////////////////////////////////////////
		//we now know that this work should be done and that there is room to put it out on the pipe
		//so do it already
		////////////////////////////////////////////////////
		//the EOF message has already been taken so no need to check 
		//all remaining messages start with the connection id
		{
		    long value = channelId;
		    final int activeMessageId = Pipe.takeMsgIdx(input);
		              	                
		    assert(peekMsgId == activeMessageId);
		    final long oldChannelId = channelId;
		    
		    long channelId2 = Pipe.takeLong(input);
		    
		    assert(oldChannelId == channelId2) : ("channel mismatch "+oldChannelId+" "+channelId2);
		    
			
			
   //TOOD: can we combine multiple message into the same  block going out to the same destination????
			
			
		    //most common case by far so we put it first
		    if (ServerResponseSchema.MSG_TOCHANNEL_100 == activeMessageId ) {
		    	            
		    	
		    	 publishDataBlock(input, output, myPipeIdx, sequenceNo, channelId2);
		    	
		    } else {
		    	
		    	throw new UnsupportedOperationException("not yet implemented "+activeMessageId);
		    	
		    } 
		}
		assert(Pipe.bytesReadBase(input)>=0);
	}


	private void skipDataBlock(final Pipe<ServerResponseSchema> sourcePipe, int idx) {
		//logger.info("processing skip");

		int meta = Pipe.takeRingByteMetaData(sourcePipe);
		int len = Pipe.takeRingByteLen(sourcePipe);
		Pipe.addAndGetBytesWorkingTailPosition(sourcePipe, len);//this does the skipping
		
		//logger.info(" new base "+Pipe.bytesReadBase(sourcePipe)+" skipped bytes "+len);
		
		Pipe.confirmLowLevelRead(sourcePipe,Pipe.sizeOf(ServerResponseSchema.instance, idx));
		Pipe.releaseReadLock(sourcePipe);
	}


	private void publishDataBlock(final Pipe<ServerResponseSchema> input, Pipe<NetPayloadSchema> output, 
			                      int myPipeIdx, int sequenceNo, long channelId) {
		

		 //////////////////////////
		 //output pipe is accumulating this data before it has even stared the message to be sent
		 //this is required in order to "skip over" the extra tags used eg "hidden" between messages by some modules.
		 /////////////////////////
		 DataOutputBlobWriter<NetPayloadSchema> outputStream = Pipe.outputStream(output);
		 DataOutputBlobWriter.openField(outputStream);
		 /////////////////////////
		
		 int expSeq = Pipe.takeInt(input); //sequence number
		 assert(sequenceNo == expSeq);
		 
		 //byteVector is payload
		 int meta = Pipe.takeRingByteMetaData(input); //for string and byte array
		 int len = Pipe.takeRingByteLen(input);
		
		         
		 int requestContext = Pipe.takeInt(input); //high 1 upgrade, 1 close low 20 target pipe	                     
		 final int blobMask = Pipe.blobMask(input);
		 byte[] blob = Pipe.byteBackingArray(meta, input);

		 int bytePosition = Pipe.bytePosition(meta, input, len); //also move the position forward
		
		 DataOutputBlobWriter.write(outputStream, blob, bytePosition, len, blobMask);
		 
		 
		 //view in the console what we just wrote out to the next stage.
		 //System.out.println("id "+myPipeIdx);
		 boolean debug = false;
		 if (debug){
			 Appendables.appendUTF8(System.err, blob, bytePosition, len, blobMask);
		 }
//		 if (ServerCoordinator.TEST_RECORDS) {
//			 //check 
//			 testValidContent(myPipeIdx, input, meta, len);
//			 
//			 //testValidContentQuick(sourcePipe, meta, len);
//		 }
		 
		 int temp = Pipe.bytesReadBase(input);
		 Pipe.confirmLowLevelRead(input, SIZE_OF_TO_CHNL);	 
		 Pipe.releaseReadLock(input);
	
		 int y = 0;
		 //If a response was sent as muliple parts all part of the same sequence number then we roll them up as a single write when possible.
		 while ( Pipe.peekMsg(input, ServerResponseSchema.MSG_TOCHANNEL_100) 
			 	 && Pipe.peekInt(input, 3) == expSeq 
			 	 && Pipe.peekLong(input, 1) == channelId 
			     && ((len+Pipe.peekInt(input, 5))<output.maxVarLen)  ) {
					 //this is still part of the current response so combine them together
					
			 		y++;
			 
					 int msgId = Pipe.takeMsgIdx(input); //msgIdx
					 long conId = Pipe.takeLong(input); //connectionId;
					 int seq = Pipe.takeInt(input);
					
					 assert(ServerResponseSchema.MSG_TOCHANNEL_100 == msgId);
					 assert(channelId == conId);
					 assert(seq == expSeq);
					 
					 
					 int meta2 = Pipe.takeRingByteMetaData(input); //for string and byte array
					 int len2 = Pipe.takeRingByteLen(input);
					 len+=len2;//keep running count so we can sure not to overflow the output

					 //logger.trace("added more length of {} ",len2);
					 
					 int bytePosition2 = Pipe.bytePosition(meta2, input, len2); //move the byte pointer forward
					 
					 DataOutputBlobWriter.write(outputStream, blob, bytePosition2, len2, blobMask);
					 						 
					 requestContext = Pipe.takeInt(input); //this replaces the previous context read
		
					 Pipe.confirmLowLevelRead(input, SIZE_OF_TO_CHNL);	 
					 
					 //Pipe.readNextWithoutReleasingReadLock(input);
					 Pipe.releaseReadLock(input);
					 
		 }
		 assert(Pipe.bytesReadBase(input)>=0);
		 
		 final long time = 0;//System.nanoTime();//field not used by server...
	
		 writeToNextStage(output, channelId, len, requestContext, blobMask, blob, bytePosition, time); 
		 
		 assert(Pipe.bytesReadBase(input)>=0);
		 //TODO: we should also look at the module definition logic so we can have mutiple OrderSuper instances (this is the first solution).
		 //Pipe.releaseAllPendingReadLock(input); //now done consuming the bytes so release the pending read lock release.
		 assert(0==Pipe.releasePendingByteCount(input));
		 assert(Pipe.bytesReadBase(input)>=0);

	}


//	private void testValidContentQuick(final Pipe<ServerResponseSchema> sourcePipe, int meta, int len) {
//		int pos = Pipe.convertToPosition(meta, sourcePipe);
//		 
//		 char ch = (char)sourcePipe.blobRing[sourcePipe.blobMask&pos];
//		 if (ch=='H') {
//			 if (89!=len) {
//				 logger.info("ERROR FORCE EXIT B {}",len);
//				 System.exit(-1);
//			 }
//			// System.err.println("h "+len);
//			 
//		 } else if (ch=='{') {			 
//			 if (30!=len) {
//				 logger.info("ERROR FORCE EXIT A {}",len);
//				 System.exit(-1);
//			 }
// 			// System.err.println("{ "+len);
//			 
//		 } else  {
//			 System.err.println(ch);
//			 logger.info("ERROR FORCE EXIT odd unexpected char in incomming pipe");
//			 System.exit(-1);
//		 }
//		
//		 char ch2 = (char)sourcePipe.blobRing[sourcePipe.blobMask&(pos+len-1)];
//		 if (ch2!=10) {
//			 System.err.println(ch2);
//			 logger.info("ERROR FORCE EXIT2");
//			 System.exit(-1);
//		 }
//	}
	
//	private void testValidContent(final int idx, Pipe<?> pipe, int meta, int len) {
//		
//		if (ServerCoordinator.TEST_RECORDS) {
//			
//			//write pipeIdx identifier.
//			//Appendables.appendUTF8(System.out, target.blobRing, originalBlobPosition, readCount, target.blobMask);
//			
//			int pos = Pipe.convertToPosition(meta, pipe);
//			    				
//			
//			boolean confirmExpectedRequests = true;
//			if (confirmExpectedRequests) {
//				Appendables.appendUTF8(accumulators[idx], pipe.blobRing, pos, len, pipe.blobMask);						    				
//				
//				while (accumulators[idx].length() >= HTTPUtil.expectedOK.length()) {
//					
//				   int c = startsWith(accumulators[idx],HTTPUtil.expectedOK); 
//				   if (c>0) {
//					   
//					   String remaining = accumulators[idx].substring(c*HTTPUtil.expectedOK.length());
//					   accumulators[idx].setLength(0);
//					   accumulators[idx].append(remaining);							    					   
//					   
//					   
//				   } else {
//					   logger.info("A"+Arrays.toString(HTTPUtil.expectedOK.getBytes()));
//					   logger.info("B"+Arrays.toString(accumulators[idx].subSequence(0, HTTPUtil.expectedOK.length()).toString().getBytes()   ));
//					   
//					   logger.info("FORCE EXIT ERROR at {} exlen {}",pos, HTTPUtil.expectedOK.length());
//					   System.out.println(accumulators[idx].subSequence(0, HTTPUtil.expectedOK.length()).toString());
//					   System.exit(-1);
//					   	
//					   
//					   
//				   }
//				
//					
//				}
//			}
//			
//			
//		}
//	}
    
//	private int startsWith(StringBuilder stringBuilder, String expected2) {
//		
//		int count = 0;
//		int rem = stringBuilder.length();
//		int base = 0;
//		while(rem>=expected2.length()) {
//			int i = expected2.length();
//			while (--i>=0) {
//				if (stringBuilder.charAt(base+i)!=expected2.charAt(i)) {
//					return count;
//				}
//			}
//			base+=expected2.length();
//			rem-=expected2.length();
//			count++;
//		}
//		return count;
//	}


	private void handshakeProcessing(Pipe<NetPayloadSchema> pipe, long channelId) {
		SSLConnection con = coordinator.connectionForSessionId(channelId);
		
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
		    		logger.info("OrderSuperviser sent handshake pos");
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


	private void writeToNextStage(Pipe<NetPayloadSchema> output, final long channelId, int len, int requestContext,
			int blobMask, byte[] blob, int bytePosition, long time) {
		/////////////
		 //if needed write out the upgrade message
		 ////////////
				 
		//logger.info("requestContext "+Integer.toBinaryString(requestContext));
		
		
		 if (0 != (UPGRADE_MASK & requestContext)) { //NOTE: must NOT use var length field, we are accumulating for plain writes
			 
			 //the next response should be routed to this new location
			 int upgSize = Pipe.addMsgIdx(output, NetPayloadSchema.MSG_UPGRADE_307);
			 Pipe.addLongValue(channelId, output);
			 Pipe.addIntValue(UPGRADE_TARGET_PIPE_MASK & requestContext, output);;
			 Pipe.confirmLowLevelWrite(output, upgSize);
			 Pipe.publishWrites(output);
			 
		 }
		                	
		 /////////////
		 //write out the content
		 /////////////
		 
		 
		 //logger.trace("write content from super to wrapper size {} for {}",len,channelId);
		 
		 int plainSize = Pipe.addMsgIdx(output, NetPayloadSchema.MSG_PLAIN_210);
		 Pipe.addLongValue(channelId, output);
		 Pipe.addLongValue(time, output);
		 Pipe.addLongValue(Pipe.getWorkingTailPosition(output), output);

		 int lenWritten = DataOutputBlobWriter.closeLowLevelField(Pipe.outputStream(output));
		 
		 //logger.trace("real len written {} ",lenWritten);
		 
		 Pipe.confirmLowLevelWrite(output, plainSize);
		 Pipe.publishWrites(output);
		 
		 if (0 != (END_RESPONSE_MASK & requestContext)) {
			//logger.info("detected end and incremented sequence number");
		    //we have finished all the chunks for this request so the sequence number will now go up by one	
		 	int idx = (int)(channelId & coordinator.channelBitsMask);
			expectedSquenceNos[idx]++;
	//		expectedSquenceNosChannelId[idx] = channelId;
		 	expectedSquenceNosPipeIdx[idx] = (short)-1;//clear the assumed pipe
		 	
		 	//logger.info("increment expected for chnl {}  to value {} len {}",channelId, expectedSquenceNos[(int)(channelId & coordinator.channelBitsMask)], len);
		 	
		 } 
		                     
		 
		 //////////////
		 //if needed write out the close connection message
		 //////////////
		 
		 if (0 != (CLOSE_CONNECTION_MASK & requestContext)) { 
			 
			 int disSize = Pipe.addMsgIdx(output, NetPayloadSchema.MSG_DISCONNECT_203);
			 Pipe.addLongValue(channelId, output);
			 Pipe.confirmLowLevelWrite(output, disSize);
			 Pipe.publishWrites(output);
			
		 }
	}
    
}
