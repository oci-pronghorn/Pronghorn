package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.config.HTTPContentTypeDefaults;
import com.ociweb.pronghorn.network.config.HTTPHeader;
import com.ociweb.pronghorn.network.config.HTTPHeaderDefaults;
import com.ociweb.pronghorn.network.config.HTTPRevisionDefaults;
import com.ociweb.pronghorn.network.config.HTTPSpecification;
import com.ociweb.pronghorn.network.config.HTTPVerbDefaults;
import com.ociweb.pronghorn.network.http.SequenceValidator;
import com.ociweb.pronghorn.network.schema.HTTPLogResponseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ServiceObjectHolder;

//TODO: should add feature of subscriptions here due to it being before the encryption stage.

/**
 * Consumes the sequence number in order and holds a pool entry for this connection.
 * Sends the data in order to the right pool entry for encryption to be applied down stream.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class OrderSupervisorStage extends PronghornStage { //AKA re-ordering stage
    
    private static final byte[] BYTES_NEWLINE = "\r\n".getBytes();
	private static final int PAYLOAD_LENGTH_IDX = 5;
	private static final int SIZE_OF_TO_CHNL = Pipe.sizeOf(ServerResponseSchema.instance, ServerResponseSchema.MSG_TOCHANNEL_100);
    public static final byte[] EMPTY = new byte[0];
    
	private static Logger logger = LoggerFactory.getLogger(OrderSupervisorStage.class);

    private final Pipe<ServerResponseSchema>[] dataToSend;
    private final Pipe<HTTPLogResponseSchema> log;
    
    private final Pipe<NetPayloadSchema>[] outgoingPipes;
        
    public static boolean showUTF8Sent = false;
        
    private int[]            expectedSquenceNosSequence;
    private short[]          expectedSquenceNosPipeIdx;
    private long[]           expectedSquenceNosChannelId;
    
    private final int channelBitsSize;
    private final int channelBitsMask;
    private final boolean isTLS;
    
    
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
	private ServiceObjectHolder<ServerConnection> socketHolder;
	private ServerConnectionStruct conStruct;
	private HTTPSpecification<HTTPContentTypeDefaults, HTTPRevisionDefaults, HTTPVerbDefaults, HTTPHeaderDefaults> spec;
	private final long[] routeSLA;

	
    public static OrderSupervisorStage newInstance(GraphManager graphManager, 
    		Pipe<ServerResponseSchema>[] inputPipes, 
    		Pipe<HTTPLogResponseSchema> log,
    		//Pipe<AlertNoticeSchema> alerts, //can be null...new pipe schema for alerts
    		Pipe<NetPayloadSchema>[] outgoingPipes, 
    		ServerCoordinator coordinator, boolean isTLS) {
    	return new OrderSupervisorStage(graphManager, inputPipes, log, outgoingPipes, coordinator);
    }

	/**
	 * Data arrives from random input pipes, but each message has a channel id and squence id.
	 * Data is ordered by sequence number and sent to the pipe from the pool belonging to that specific channel id
	 * @param graphManager
	 * @param inputPipes _in_ The server response which will be supervised.
	 * @param log _out_ The log output pipe.
	 * @param outgoingPipes _out_ The net payload after order is enforced.
	 * @param coordinator
	 * @param isTLS
	 */
    public OrderSupervisorStage(GraphManager graphManager, 
    		                     Pipe<ServerResponseSchema>[][] inputPipes, 
    		                     Pipe<HTTPLogResponseSchema> log,
    		                     Pipe<NetPayloadSchema>[] outgoingPipes,
    		                     ServerCoordinator coordinator, boolean isTLS) {
    	this(graphManager, join(inputPipes), log, outgoingPipes, coordinator);
    }

    private final int pipeDivisor; //to take out the factors above like track and sub track
    
    public OrderSupervisorStage(GraphManager graphManager, 
    		Pipe<ServerResponseSchema>[] inputPipes, 
    		Pipe<HTTPLogResponseSchema> log,
    		Pipe<NetPayloadSchema>[] outgoingPipes, 
    		ServerCoordinator coordinator) {
        super(graphManager, inputPipes, null==log ? outgoingPipes : join(outgoingPipes,log));           
        
        this.channelBitsMask = coordinator.channelBitsMask;
        this.channelBitsSize = coordinator.channelBitsSize;
        this.isTLS = coordinator.isTLS;
        this.socketHolder = ServerCoordinator.getSocketChannelHolder(coordinator);
        
        this.conStruct = coordinator.connectionStruct();
        this.spec = coordinator.spec;
                
        //moduleParallelism is all the routes which is Readers * sub tracks so it must have both primes in use.
        this.pipeDivisor = coordinator.moduleParallelism();
        
        this.routeSLA = coordinator.routeSLALimits;
        
        this.dataToSend = inputPipes;
        this.log = log;
        
        assert(outgoingPipes.length>0);
        
        assert(dataToSend.length<=Short.MAX_VALUE) : "can not support more pipes at this time. This code will need to be modified";
        if (dataToSend.length>Short.MAX_VALUE) {
        	throw new UnsupportedOperationException("can not support more pipes at this time. This code will need to be modified");
        }
        
        this.outgoingPipes = outgoingPipes;

        this.poolMod = outgoingPipes.length;
        this.shutdownCount = dataToSend.length;
        
        this.supportsBatchedPublish = false;
        this.supportsBatchedRelease = false;
        
        //TODO: this should be a warning
//        if (minVarLength(outgoingPipes) < maxVarLength(inputPipes)) {
//          	throw new UnsupportedOperationException(
//        			"Increase the size of setMaxResponseSize() value.\nAll output<NetPayloadSchema> pipes must support variable length fields equal to or larger"
//        			+ " than all input<ServerResponseSchema> pipes. out "+minVarLength(outgoingPipes)+" in "+maxVarLength(inputPipes)
//        			);
//        }
        
        this.maxOuputSize = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210) +
        								Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_UPGRADE_307) +            
        								Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_DISCONNECT_203);
        
         GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
        //NOTE: do not flag order super as a LOAD_MERGE since it must be combined with
        //      its feeding pipe as frequently as possible, critical for low latency.
        
        //GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
         
    }
    


	@Override
    public void startup() {
		
		//Due to N Order Supervisors in place this array is far too large.
		int totalChannels = channelBitsSize; //WARNING: this can be large eg 4 million
        expectedSquenceNosSequence = new int[totalChannels];//room for 1 per active channel connection
        
        expectedSquenceNosPipeIdx = new short[totalChannels];
        Arrays.fill(expectedSquenceNosPipeIdx, (short)-1);

        expectedSquenceNosChannelId = new long[totalChannels];
        Arrays.fill(expectedSquenceNosChannelId, (long)-1);

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

	    	Pipe<ServerResponseSchema>[] localPipes = dataToSend;

			int c = localPipes.length;
				
		    while (--c >= 0) {
		        	if (Pipe.hasContentToRead(localPipes[c])) {
		        		processPipe(localPipes[c], c);//we read as much as we can from here		        		
		        	}		        	
		    }		       
		
    }

	
	//HashMap<Integer,Integer> lastPipe = new HashMap<Integer,Integer>(); 
	
	
	private void processPipe(final Pipe<ServerResponseSchema> sourcePipe, int pipeIdx) {
		
		int blockedCount = 0;
		while (Pipe.contentRemaining(sourcePipe)>0 && blockedCount<10) {
				
			assert(Pipe.bytesReadBase(sourcePipe)>=0);
			
		    //peek to see if the next message should be blocked, eg out of order, if so skip to the next pipe
		    int peekMsgId = Pipe.peekInt(sourcePipe, 0);		
		    long channelId = -2;
		    
		    if (peekMsgId>=0 
		    	&& ServerResponseSchema.MSG_SKIP_300!=peekMsgId 
		    	&& (channelId=Pipe.peekLong(sourcePipe, 1))>=0) {
    	
		    	//ensure that the channelId is not a factor of the roots found in poolMod then take the mod	    	
		    	int myPipeIdx = (int)((channelId/pipeDivisor) % poolMod);//channel must always have the same pipe for max write speed. 
		    	//single pipe in is split into the right destination pipes
		    	
//		    	Integer last = lastPipe.get(myPipeIdx);
//		    	if (null!=last) {
//		    		if (last.intValue()!=(int)channelId) {
//		    			System.out.println("internal error, "+myPipeIdx+" was "+last.intValue()+" now "+channelId);
//		    		}
//		    	}		    	
//		    	lastPipe.put(myPipeIdx, (int)channelId);
//    	//System.out.println("write: "+channelId+" to pipe "+myPipeIdx+" divisor "+pipeDivisor+" mod pool "+poolMod);
		    	
		        if (Pipe.hasRoomForWrite(outgoingPipes[myPipeIdx], maxOuputSize) && (log==null || Pipe.hasRoomForWrite(log))) {	
				    			        	
			    	//only after we know that we are doing something.
		        	if (!processInputData(sourcePipe, pipeIdx,  
									        		peekMsgId, myPipeIdx,
													channelId)) {
		        		//logger.trace("stall on sequence number or this is shutdown");
		        		return;//shutting down OR the next message is not the right sequence number.
		        	};
		        	blockedCount = 0;//reset since we found some data
		        	
		    	} else {
		    		//keep reading until we get N no rooms in a row. or we have no more content.
		    		Thread.yield();
		    		blockedCount++;
					///////////////////////////////
			        //quit early if the pipe is full, NOTE: the order super REQ long output pipes
			        ///////////////////////////////
		    		assert(Pipe.bytesReadBase(sourcePipe)>=0);
	//	    		keepWorking = false; //break out to allow down stream to consume
		    		//next item on pipe has no place to write so we are stalled...
		    	}

		    } else {
		    	assert(ServerResponseSchema.MSG_SKIP_300==peekMsgId);
		    	//if this was to be skipped then do it
		    	//if ((ServerResponseSchema.MSG_SKIP_300==peekMsgId) || (-1==peekMsgId)) {		    	
		    		skipData(sourcePipe, channelId);
		    	//} else {
		    	//	logger.error("unknown message type");
		    	//	break;
		    	//}
		    }
		}
		//this should be flat with 16 max if we have 16 in flight?
		//we have 1 pipe coming in with 16*592 requests but these are going to 592 different places
		
		//System.out.println(" blocked count "+blockedCount+" pipe idx "+pipeIdx);
	}

	SequenceValidator validator = new SequenceValidator();
	
	private boolean processInputData(final Pipe<ServerResponseSchema> sourcePipe, int pipeIdx,
			int peekMsgId, int myPipeIdx, long channelId) {
		
		
		int sequenceNo = Pipe.peekInt(sourcePipe,  3);	
			 
		//read the next non-blocked pipe, sequenceNo is never reset to zero
		//every number is used even if there is an exception upon write.
     
		int idx = (int)(channelId & channelBitsMask);
		
		///////////////////
		//clear when we discover a new connection
		///////////////////
		if ((expectedSquenceNosChannelId[idx]!=channelId) || (sequenceNo==0)) {

			expectedSquenceNosSequence[idx] = 0;
			expectedSquenceNosChannelId[idx] = channelId;
			
		}

		return checkSeqAndProcessInput(sourcePipe, pipeIdx,  
				peekMsgId, myPipeIdx, channelId,
				sequenceNo, idx, expectedSquenceNosSequence[idx]);
	
	}

	private boolean checkSeqAndProcessInput(final Pipe<ServerResponseSchema> sourcePipe, int pipeIdx, 
			int peekMsgId, int myPipeIdx, long channelId, int sequenceNo, int idx,
			int expected) {
			
		boolean keepRunning = true;
		if ((0==sequenceNo) || (sequenceNo >= expected)) {
			if ((0==sequenceNo) || (expected == sequenceNo)) {
				//stop if we get a shutdown
				keepRunning = processExpectedSequenceValue(sourcePipe, pipeIdx,
						peekMsgId,
						myPipeIdx, sequenceNo, 
						channelId, idx, expected);
				
				//System.out.println("sequence matched "+sequenceNo);
		    	
			} else {
				//stop if the sequence number does not match
				keepRunning = false;//sequenceNo<=expected;//false;
				//System.out.println("for channel:"+channelId+" waiting for: "+expected+" but got "+sequenceNo );
				
				assert(hangDetect(pipeIdx, sequenceNo, channelId, expected));
				assert(sequenceNo>expected) : "found smaller than expected sequenceNo, they should never roll back";
				assert(Pipe.bytesReadBase(sourcePipe)>=0);
		
			}
		} else {
			 keepRunning = false;
			 badSequenceNumberProcessing(channelId, sequenceNo, expected);
		}
		return keepRunning;
		
	}

	private boolean badSequenceNumberProcessing(long channelId, int sequenceNo, int expected) {
		
		//we already moved past this point
		//this sequence just read is < the expected value
		BaseConnection con = socketHolder.get(channelId);
		if (null!=con) {
			con.close();
		}
		logger.warn("Corrupt data detected, connection closed. Expected next sequence of {} but got {} which is too old. "
					,expected ,sequenceNo);
		return false;
	}
	
	private boolean processExpectedSequenceValue(final Pipe<ServerResponseSchema> sourcePipe,
			int pipeIdx,
			int peekMsgId, int myPipeIdx, int sequenceNo,
			long channelId, int idx, int expected) {
		
		boolean keepWorking = true;
		
		//logger.trace("found expected sequence {}",sequenceNo);
		final short expectedPipe = expectedSquenceNosPipeIdx[idx];
		if (-1 == expectedPipe) {
			expectedSquenceNosPipeIdx[idx]=(short)pipeIdx;
			int responses = copyDataBlock(sourcePipe, pipeIdx, peekMsgId, 
					myPipeIdx, sequenceNo,
					channelId, true);
			expectedSquenceNosSequence[idx]+=responses;
			
		} else {			        			
			if (expectedPipe ==(short)pipeIdx) {
				int responses = copyDataBlock(sourcePipe, pipeIdx, peekMsgId, 
						myPipeIdx, sequenceNo,
						channelId, false);
				expectedSquenceNosSequence[idx]+=responses;
				
			} else {
				
				if (shutdownInProgress) {
					keepWorking = false; //break out
				} else {
					assert(hangDetect(pipeIdx, sequenceNo, channelId, expected));
					//drop the data
					//logger.info("skipped older response B Pipe:{} vs Pipe:{} ",expectedSquenceNosPipeIdx[idx],pipeIdx);
					Pipe.skipNextFragment(sourcePipe);
					//will drop out bottom and return to the top to continue;
				}
			}
		}
		return keepWorking;
	}


	private int copyDataBlock(final Pipe<ServerResponseSchema> sourcePipe, int pipeIdx, int peekMsgId,
			int myPipeIdx, int sequenceNo, long channelId, boolean beginningOfResponse) {
		
		failureIterations = -1;//clears the flag to let hang detector know we are ok.			        		

		Pipe<NetPayloadSchema> outPipe = outgoingPipes[myPipeIdx];
		
		assert(recordInputs(channelId, sequenceNo, pipeIdx));			        		
		if (!isTLS) {
		} else {
			finishHandshake(outPipe, channelId);
		}
		assert(Pipe.bytesReadBase(sourcePipe)>=0);				
		int responses = copyDataBlock(sourcePipe, peekMsgId, myPipeIdx, sequenceNo, channelId, beginningOfResponse);							
		assert(Pipe.bytesReadBase(sourcePipe)>=0);
		return responses;
	}

	private void skipData(final Pipe<ServerResponseSchema> sourcePipe, long channelId) {
		////////////////
		//these consume data but do not write out to pipes
		////////////////
		int idx = Pipe.takeMsgIdx(sourcePipe);
		
		if (-1 != idx) {
			
			if (channelId!=-2) {
				logger.warn("skipping channel data, id was {}",channelId);
			}
			
			assert(Pipe.bytesReadBase(sourcePipe)>=0);
			Pipe.skipNextFragment(sourcePipe, idx);
			assert(Pipe.bytesReadBase(sourcePipe)>=0);
			//at bottom will continue;
		} else {	
			
			assert(-1 == idx) : "unexpected value";
			Pipe.confirmLowLevelRead(sourcePipe, Pipe.EOF_SIZE);
			Pipe.releaseReadLock(sourcePipe);
			
			if (--shutdownCount<=0) {
				shutdownInProgress = true;
				assert(Pipe.bytesReadBase(sourcePipe)>=0);
				//no messages are on source pipe so will naturally break;
			} else {
				//logger.trace("dec shutdown count");
				assert(Pipe.bytesReadBase(sourcePipe)>=0);
				//at bottom will continue;
			}
		}
	}

	private void finishHandshake(Pipe<NetPayloadSchema> outPipe, long channelId) {
		BaseConnection con = socketHolder.get(channelId);			
		if (!SSLUtil.handshakeProcessing(outPipe, con)) {
			//TODO: we must wait until later...
		}
	}

	private boolean hangDetect(int pipeIdx, int sequenceNo, long channelId, int expected) {

		////////////
		//disabled until we can find a better way to compute this
		//this works but requires a long wait window and we know not how long to wait.
		////////////
//		if (failureIterations==100_000_000) { //equals so we only do this once.
//
//	            assert(recordInputs(channelId, sequenceNo, pipeIdx));
//	        
//				logger.warn("Hang detected, Critical internal error must shutdown.");
//				logger.info("looking for {} but got {} for connection {} on idx {}",
//							expected, sequenceNo, channelId, pipeIdx);
//				logger.info("jumped ahead a total of {} ",movedUpCount);
//				if (null!=recordChannelId) {
//					//we have the most recent history so do display it.
//					displayRecentRequests();
//				}
//				shutdownInProgress = true;
//		} else {
//			failureIterations++;
//		}
		return true;
	}

    ////////////////////////////////////
	///used when assert is on to record the last few data points for review
	////////////////////////////////////
	int RECORD_BITS = 4;
	int RECORD_SIZE = 1<<RECORD_BITS;
	int RECORD_MASK = RECORD_SIZE-1;
	long recordPosition = 0;
	long[] recordChannelId;
	int[]  recordSequenceNo;
	int[]  recordPipeIdx;
    
    long movedUpCount = 0;
    int  failureIterations = 0;
     
    
	private boolean recordInputs(long channelId, int sequenceNo, int pipeIdx) {
		if (null==recordChannelId) {
			recordChannelId = new long[RECORD_SIZE];
			recordSequenceNo = new int[RECORD_SIZE];
			recordPipeIdx = new int[RECORD_SIZE];
		}
		
		recordChannelId[(int)recordPosition&RECORD_MASK] = channelId;
		recordSequenceNo[(int)recordPosition&RECORD_MASK] = sequenceNo;
		recordPipeIdx[(int)recordPosition&RECORD_MASK] = pipeIdx;
		recordPosition++;
		return true;
	}
	
	private void displayRecentRequests() {
		long start = recordPosition-1;
		long limit = Math.max(0, recordPosition-RECORD_MASK);
		
		Appendable target = System.err;
		while (start>=limit) {
		
			try {
				Appendables.appendValue(target.append("#"),start).append(' ');
				Appendables.appendValue(target.append("Chnl:"),recordChannelId[(int)start&RECORD_MASK]).append(' ');
				Appendables.appendValue(target.append("Seq:"),recordSequenceNo[(int)start&RECORD_MASK]).append(' ');
				Appendables.appendValue(target.append("Piped:"),recordPipeIdx[(int)start&RECORD_MASK]).append(' ');
				target.append('\n');				
				
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			start--;
		}
		
	}

	private int copyDataBlock(final Pipe<ServerResponseSchema> input, int peekMsgId,
							   int myPipeIdx,
							   int sequenceNo, long channelId, boolean beginningOfResponse) {
		 
		assert(Pipe.bytesReadBase(input)>=0);
		
		
		////////////////////////////////////////////////////
		//we now know that this work should be done and
		//that there is room to put it out on the pipe
		//so do it already
		////////////////////////////////////////////////////
		//the EOF message has already been taken so no need to check 
		//all remaining messages start with the connection id
		//////////////////////////////////////////
	    final int activeMessageId = Pipe.takeMsgIdx(input);		              	                
		assert(peekMsgId == activeMessageId);
		
		final long oldChannelId = channelId;		    
		long channelId2 = Pipe.takeLong(input);		    
		assert(oldChannelId == channelId2) : ("channel mismatch "+oldChannelId+" "+channelId2);
		
		int totalResponses = 0;
	    //most common case by far so we put it first
	    if (ServerResponseSchema.MSG_TOCHANNEL_100 == activeMessageId ) {

	    	 totalResponses = publishDataBlock(input, myPipeIdx, sequenceNo, 
	    			                           channelId2, beginningOfResponse, 
	    			                           outgoingPipes[myPipeIdx], socketHolder.get(channelId), this);
	    	
	    } else {
	    	
	    	//TOOD: can we combine multiple message into the same  block going out to the same destination????
	    	throw new UnsupportedOperationException("not yet implemented "+activeMessageId);
	    	
	    } 
	
		assert(Pipe.bytesReadBase(input)>=0);
		return totalResponses;
	}


	private static int publishDataBlock(final Pipe<ServerResponseSchema> input,
			                      final int myPipeIdx, 
			                      final int sequenceNo,
			                      final long channelId, 
			                      final boolean beginningOfResponse,
			                      final Pipe<NetPayloadSchema> output,
			                      final BaseConnection connection,
			                      final OrderSupervisorStage that) {
		
		 //////////////////////////
		 //output pipe is accumulating this data before it has even stared the message to be sent
		 //this is required in order to "skip over" the extra tags used eg "hidden" between messages by some modules.
		 /////////////////////////
		 DataOutputBlobWriter<NetPayloadSchema> outputStream = Pipe.openOutputStream(output);
			 
		 final int expSeq = Pipe.takeInt(input); //sequence number
		 assert(sequenceNo == expSeq);
		 
		 //byteVector is payload
		 final int meta = Pipe.takeByteArrayMetaData(input); //for string and byte array
		 final int len = Pipe.takeByteArrayLength(input);
		
		  //high 1 upgrade, 1 close low 20 target pipe	                     
 
		 
		  //also move the position forward
		
		 return loadConnectionDataAndPublish(that, input, myPipeIdx, channelId, beginningOfResponse, 
				 	output, outputStream, expSeq,
				 	len, Pipe.takeInt(input), Pipe.byteBackingArray(meta, input), 
				 	Pipe.bytePosition(meta, input, len), connection);

	}

	private static int loadConnectionDataAndPublish(
			final OrderSupervisorStage that,
			final Pipe<ServerResponseSchema> input, final int myPipeIdx, long channelId,
			final boolean beginningOfResponse, final Pipe<NetPayloadSchema> output,
			final DataOutputBlobWriter<NetPayloadSchema> outputStream, final int expSeq, final int len, final int requestContext,
			final byte[] blob, final int bytePosition, final BaseConnection con) {
		
		 if (null!=con) {
			 channelId = con.id;	
			 
			 if (con.hasDataRoom() && that.conStruct!=null) {
				 //TODO: echo feature
				 
				 //This echo feature can not be correct and must be re-implemented??...
				 //should be after write not before??? and should read from reader???
				 //if (beginningOfResponse) {
				 //	 len = echoHeaders(outputStream, len, Pipe.blobMask(input), blob, bytePosition, reader);
				 //}
				 
			 }
			
		 } 
		 
		 if (len>0) {
			 DataOutputBlobWriter.write(outputStream, blob, bytePosition, len, Pipe.blobMask(input));
		 }
		 Pipe.confirmLowLevelRead(input, SIZE_OF_TO_CHNL);	 
		 Pipe.releaseReadLock(input);

		 if (null!=con) {
			 return rollupMultiAndPublish(that, input, myPipeIdx, channelId, output,
					 outputStream, expSeq, len, requestContext, Pipe.blobMask(input), blob,
					 con, that.log);
		 } else {
			 return 0;
		 }
		
	}

	private static int rollupMultiAndPublish(
			OrderSupervisorStage that,
			final Pipe<ServerResponseSchema> input, int myPipeIdx, long channelId,
			Pipe<NetPayloadSchema> output, DataOutputBlobWriter<NetPayloadSchema> outputStream,
			int expSeq, int len, int requestContext, final int blobMask, byte[] blob,
			final BaseConnection con, Pipe<HTTPLogResponseSchema> outLog) {
		
		 boolean finishedFullReponse = true;//only false when we run out of room...
		 int localContext;
		 
		 int totalResponses = 0;	
		 int logPos = 0;
		 int logLen = Pipe.outputStream(output).length();
		 
		 //TODO: need to capture which pipe this came in from and if it violates the SLA.
		 //      then report this as an error.
		 
		 if (0 != (END_RESPONSE_MASK & requestContext)) {
			 totalResponses++;
				 
			 writeToLog(channelId, output, expSeq, logPos, logLen, con.readStartTime(), outLog);			 

			 if (con.hasHeadersToEcho()) {
				 //write the echos	??		 
				 
				 //This echo feature can not be correct and must be re-implemented??...
				 //should be after write not before??? and should read from reader???
				 //	if (beginningOfResponse) {
				 //	 len = echoHeaders(outputStream, len, Pipe.blobMask(input), blob, bytePosition, reader);
				 
				 //}
				 
			 }
			
			 
			 logPos+=logLen;
			 logLen=0;
			 
			 
		 }
		 
		 int tseq = -1;
		 //If a response was sent as multiple parts all part of the same sequence number then we roll them up as a single write when possible.
		 while ( Pipe.peekMsg(input, ServerResponseSchema.MSG_TOCHANNEL_100) 
			 	 && 
			 	  (
			 	   ((tseq=Pipe.peekInt(input, 0xFF&ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_SEQUENCENO_23)) == expSeq)
			 	   || (tseq==expSeq+1)
			 	  )
			 	 && Pipe.peekLong(input, 0xFF&ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_CHANNELID_21) == channelId 
			     && (finishedFullReponse =((len+Pipe.peekInt(input, PAYLOAD_LENGTH_IDX))<output.maxVarLen)) ) {
					 //this is still part of the current response so combine them together
						
			         expSeq = tseq;

					 int msgId = Pipe.takeMsgIdx(input); //msgIdx
					 long conId = Pipe.takeLong(input); //connectionId;
					 int seq = Pipe.takeInt(input);
					
					 assert(ServerResponseSchema.MSG_TOCHANNEL_100 == msgId);
					 assert(channelId == conId);
					 assert(seq == expSeq);
					 
					 
					 int meta2 = Pipe.takeByteArrayMetaData(input); //for string and byte array
					 int len2 = Pipe.takeByteArrayLength(input);
					 assert(len2>=0);
					 len+=len2;//keep running count so we can sure not to overflow the output
					 logLen+=len2;
					 int bytePosition2 = Pipe.bytePosition(meta2, input, len2); //move the byte pointer forward
					 
					 //logger.info("read position {}",bytePosition2);
					 
					 DataOutputBlobWriter.write(outputStream, blob, bytePosition2, len2, blobMask);
					 	
					 localContext = Pipe.takeInt(input);
					 if (0 != (END_RESPONSE_MASK & localContext)) {
						 totalResponses++;
						 
						 long startTime = con.readStartTime();
						 //TODO: need to capture which pipe this came in from and if it violates the SLA.
						 //      then report this as an error.
						 
						 writeToLog(channelId, output, expSeq, logPos, logLen, startTime, outLog);
						 
						 requestContext = localContext;
						 if (con.hasHeadersToEcho()) {
							 //TODO: do echos...
							 
						 }
					
						 logPos+=logLen;
						 logLen=0;
						 
						 
						 
					 }
					 
					 //note: these masks are or'd with the previous
					 //upgrade, end-response, and close connection apply the same 
					 requestContext |= localContext;
										 
					 Pipe.confirmLowLevelRead(input, SIZE_OF_TO_CHNL);	 
					 
					 Pipe.releaseReadLock(input);
					 
		 }
		 assert(Pipe.bytesReadBase(input)>=0);
		 
		 if (finishedFullReponse) {
			 //if finished and we had some responses
			 if (totalResponses>0) {//NOTE: this would be much nicer up above to avoid reconstructing IDX..
				 int idx = (int)(channelId & that.channelBitsMask);
				 that.expectedSquenceNosPipeIdx[idx] = (short)-1;//clear the assumed pipe
			 }
		 }
				
		 if (showUTF8Sent) {
			 Pipe.outputStream(output).debugAsUTF8();
		 }
		
		 writeToNextStage(output, myPipeIdx, channelId, requestContext, expSeq); 	 
		
		 assert(Pipe.bytesReadBase(input)>=0);
		 assert(0==Pipe.releasePendingByteCount(input));
		 assert(Pipe.bytesReadBase(input)>=0);
		 return totalResponses;
	}

	private static void writeToLog(final long channelId, Pipe<NetPayloadSchema> output,
			final int expSeq, int logPos, int logLen, long startTime, Pipe<HTTPLogResponseSchema> outLog) {

		 if (null!=outLog) {
			 
			 long now = System.nanoTime();
			 long durationNS = now-startTime;
			 
			 //if (businessDuration>limitSLA) {
			 //send the routeId to those listening that we have a violation					 
			 //TODO: publish if pipe is found..	
			 //}
			 
			 writeToLog(channelId, output, logPos, logLen, expSeq, now, durationNS, outLog);
		 }
	}

	private static void writeToLog(long channelId, Pipe<NetPayloadSchema> output, int outPos, int outLen,
			                       final int expSeq, long now,
			                       long serverDuration, Pipe<HTTPLogResponseSchema> outLog) {
		//if we have a log pipe then log these events
		
		 if (null!=outLog) {
			 //the log max var len may not be large enough for the entire payload
			 //so this may need to write multiple messages to capture all the data.
			 
			 DataOutputBlobWriter<?> logStr = Pipe.openOutputStream(outLog);				 
			 Pipe.outputStream(output).replicate(logStr, outPos, outLen);
			 outLog.closeBlobFieldWrite();
			 
			 int writeLength = logStr.length();
			 int startPos = Pipe.getWorkingBlobHeadPosition(outLog);
			 do {
				 //we do not know how many will be needed so we use presume to block as needed.
				 Pipe.presumeRoomForWrite(outLog);
				 
				 int size = Pipe.addMsgIdx(outLog, HTTPLogResponseSchema.MSG_RESPONSE_1);
				 Pipe.addLongValue(now, outLog);
				 Pipe.addLongValue(channelId, outLog);
				 Pipe.addIntValue(expSeq, outLog);
				 
				 int blockLen = Math.min(writeLength, outLog.maxVarLen);
				 Pipe.addBytePosAndLenSpecial(outLog, startPos, blockLen);
				 startPos += blockLen;
				 writeLength -= blockLen;
				 
				 Pipe.addLongValue(serverDuration, outLog);
				 Pipe.confirmLowLevelWrite(outLog, size);
				 Pipe.publishWrites(outLog);
			 } while (writeLength>0);
			 
		 }
	}

	private int echoHeaders(DataOutputBlobWriter<NetPayloadSchema> outputStream, int len, final int blobMask,
			byte[] blob, final int bytePosition, ChannelReader reader) {
		HTTPHeader[] headersToEcho = conStruct.headersToEcho();
		 if (null!=headersToEcho) {

			 int newLinePos = (bytePosition+len-2);
			 assert(blob[newLinePos&blobMask]=='\r');
			 assert(blob[(1+newLinePos)&blobMask]=='\n');						 
			 if (blob[newLinePos&blobMask]=='\r') {
				 len-=2;//confirm it is \r\n? add assert!
				 DataOutputBlobWriter.write(outputStream, blob, bytePosition, len, blobMask);
				 len = 0;//so the following write will not write a second time.

				 for(int i=0; i<headersToEcho.length; i++) {
					 HTTPHeader header = headersToEcho[i];
				
					 if (!reader.structured().isNull(header)) {	
						 System.err.println("echo header "+header);
						 spec.writeHeader(outputStream, header, reader.structured().read(header));
					 }
					 //TODO: confirm works with chunked and not
					 
				 }
				 outputStream.write(BYTES_NEWLINE);
			 }
		 }
		return len;
	}

	private static int writeToNextStage(Pipe<NetPayloadSchema> output, int myPipeIdx,
			final long channelId, int requestContext, int expSeq) {
		/////////////
		 //if needed write out the upgrade message
		 ////////////
				 
		//logger.info("requestContext "+Integer.toBinaryString(requestContext));
		
		
		 if (0 != (UPGRADE_MASK & requestContext)) { //NOTE: must NOT use var length field, we are accumulating for plain writes
			 
			 //the next response should be routed to this new location
			 int upgSize = Pipe.addMsgIdx(output, NetPayloadSchema.MSG_UPGRADE_307);
			 Pipe.addLongValue(channelId, output);
			 Pipe.addIntValue(UPGRADE_TARGET_PIPE_MASK & requestContext, output);
			 Pipe.confirmLowLevelWrite(output, upgSize);
			 Pipe.publishWrites(output);
			 
		 }
	
		 /////////////
		 //write out the content
		 /////////////

		 //logger.trace("write content from super to wrapper size {} for {}",len,channelId);
		 
		 int plainSize = Pipe.addMsgIdx(output, NetPayloadSchema.MSG_PLAIN_210);
		 Pipe.addLongValue(channelId, output);
		 Pipe.addLongValue(0, output); //TODO: is this time field needed to be sent? no....
		 Pipe.addLongValue(Pipe.getWorkingTailPosition(output), output);

	 
		 int lenWritten = DataOutputBlobWriter.closeLowLevelField(Pipe.outputStream(output));
		 assert(lenWritten>0) : "Do not send messages which contain no data, this is a waste";
		 		 
		 Pipe.confirmLowLevelWrite(output, plainSize);	
		 //publish and copy the bytes into the direct byte buffer for consumption outside the JVM
		 Pipe.publishWritesDirect(output);
		 
		 
	 	//////////////
	 	//if needed write out the close connection message
	 	//can only be done when we are at the end of the message
	 	//////////////
	 	if (0 != (CLOSE_CONNECTION_MASK & requestContext)) { 
	 		
	 		//logger.info("context is closed so sending disconnect to the ServerSocketWriter for connection {}",channelId);
	 		
	 		int disSize = Pipe.addMsgIdx(output, NetPayloadSchema.MSG_DISCONNECT_203);
	 		Pipe.addLongValue(channelId, output);
	 		Pipe.confirmLowLevelWrite(output, disSize);
	 		Pipe.publishWrites(output);
	 		
	 	}

	 	
		 return lenWritten;
	}
    
}
