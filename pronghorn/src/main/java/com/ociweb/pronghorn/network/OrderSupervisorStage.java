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
import com.ociweb.pronghorn.network.schema.HTTPLogResponseSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.ChannelReaderController;
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
        
    private boolean showUTF8Sent = false;
        
    private int[]            expectedSquenceNos;
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
        
        if (minVarLength(outgoingPipes) < maxVarLength(inputPipes)) {
          	throw new UnsupportedOperationException(
        			"Increase the size of setMaxResponseSize() value.\nAll output<NetPayloadSchema> pipes must support variable length fields equal to or larger"
        			+ " than all input<ServerResponseSchema> pipes. out "+minVarLength(outgoingPipes)+" in "+maxVarLength(inputPipes));
        }
        
        this.maxOuputSize = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210) +
        								Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_UPGRADE_307) +            
        								Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_DISCONNECT_203);
        
         GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
        //NOTE: do not flag order super as a LOAD_MERGE since it must be combined with
        //      its feeding pipe as frequently as possible, critical for low latency.
        

         
    }
    


	@Override
    public void startup() {
		
		//Due to N Order Supervisors in place this array is far too large.
		int totalChannels = channelBitsSize; //WARNING: this can be large eg 4 million
        expectedSquenceNos = new int[totalChannels];//room for 1 per active channel connection
        
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
		
	    	boolean haveWork;
	    	int maxIterations = 10_000;
	    	Pipe<ServerResponseSchema>[] localPipes = dataToSend;
	    	do {
		    	haveWork = false;
				int c = localPipes.length;
				int x = 0;
		        while (--c >= 0) {		        	
		        	if (!Pipe.hasContentToRead(localPipes[c])) {
		        		x++;		        		
		        	} else {
		        		//has full message
		        		haveWork |= processPipe(localPipes[c], c);		        		
		        	}
		        }  
		   
		        
	    	} while (--maxIterations>0 && haveWork && !shutdownInProgress);
	    	
    }


	private boolean processPipe(final Pipe<ServerResponseSchema> sourcePipe, int pipeIdx) {
		
		boolean didWork = false;
		boolean keepWorking = true;
		while (Pipe.contentRemaining(sourcePipe)>0 && keepWorking) {
				
			assert(Pipe.bytesReadBase(sourcePipe)>=0);
			
		    //peek to see if the next message should be blocked, eg out of order, if so skip to the next pipe
		    int peekMsgId = Pipe.peekInt(sourcePipe, 0);		
		    long channelId = -2;
		    
		    if (peekMsgId>=0 
		    	&& ServerResponseSchema.MSG_SKIP_300!=peekMsgId 
		    	&& (channelId=Pipe.peekLong(sourcePipe, 1))>=0) {
		    			  
		    	//dataToSend.length
		    	int myPipeIdx = (int)(channelId % poolMod);
		        if (Pipe.hasRoomForWrite(outgoingPipes[myPipeIdx], maxOuputSize) &&
		            (log==null || Pipe.hasRoomForWrite(log))) {	
				    	
			    	//only after we know that we are doing something.
					didWork = true;
	
			        keepWorking = processInputData(sourcePipe, pipeIdx, keepWorking, 
									        		peekMsgId, myPipeIdx,
													channelId);
		    	} else {
			        ///////////////////////////////
			        //quit early if the pipe is full, NOTE: the order super REQ long output pipes
			        ///////////////////////////////
		    		assert(Pipe.bytesReadBase(sourcePipe)>=0);
		    		keepWorking = false; //break out
		    		//logger.info("no room to write out, try again later");
		    		//return didWork;
		    	}

		    } else {		 
		    	didWork = true;
		    	//if this was to be skipped then do it
		    	if ((ServerResponseSchema.MSG_SKIP_300==peekMsgId) || (-1==peekMsgId)) {
		    		skipData(sourcePipe, channelId);
		    	} else {
		    		break;
		    	}
		    }
		}
		return didWork;
	}

	private boolean processInputData(final Pipe<ServerResponseSchema> sourcePipe, int pipeIdx, boolean keepWorking,
			int peekMsgId, int myPipeIdx, long channelId) {
		
		
		int sequenceNo = Pipe.peekInt(sourcePipe,  3);	                   
				 
		//read the next non-blocked pipe, sequenceNo is never reset to zero
		//every number is used even if there is an exception upon write.
     
		int idx = (int)(channelId & channelBitsMask);
		
		///////////////////
		//clear when we discover a new connection
		///////////////////
		if ((expectedSquenceNosChannelId[idx]!=channelId) || (sequenceNo==0)) {

			expectedSquenceNos[idx] = 0;
			expectedSquenceNosChannelId[idx] = channelId;
			
		}

		return processInput(sourcePipe, pipeIdx, keepWorking, 
				peekMsgId, myPipeIdx, channelId,
				sequenceNo, idx, expectedSquenceNos[idx]);
	
	}

	private boolean processInput(final Pipe<ServerResponseSchema> sourcePipe, int pipeIdx, boolean keepWorking,
			int peekMsgId, int myPipeIdx, long channelId, int sequenceNo, int idx,
			int expected) {
	
		if ((0==sequenceNo) || (sequenceNo >= expected)) {
			if ((0==sequenceNo) || (expected == sequenceNo)) {
				keepWorking = processExpectedSequenceValue(sourcePipe, pipeIdx,
						keepWorking, peekMsgId,
						myPipeIdx, sequenceNo, 
						channelId, idx, expected);
		    	
			} else {
	
				assert(hangDetect(pipeIdx, sequenceNo, channelId, expected));
				assert(sequenceNo>expected) : "found smaller than expected sequenceNo, they should never roll back";
				assert(Pipe.bytesReadBase(sourcePipe)>=0);
				keepWorking = false;
			}
		} else {
			
			System.out.println("B unexpected   con:"+channelId+"  seq:"+sequenceNo+" ------------");
			System.exit(-1);
			
			keepWorking = badSequenceNumberProcessing(channelId, sequenceNo, expected);
		}
		
		return keepWorking;
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


	public static boolean showCounts = false;
    
	int countOfMessages = 0;
	int countOfBlocks = 0;
	
	private boolean processExpectedSequenceValue(final Pipe<ServerResponseSchema> sourcePipe,
			int pipeIdx,
			boolean keepWorking, int peekMsgId, int myPipeIdx, int sequenceNo,
			long channelId, int idx, int expected) {
	
		//logger.trace("found expected sequence {}",sequenceNo);
		final short expectedPipe = expectedSquenceNosPipeIdx[idx];
		if (-1 == expectedPipe) {
			expectedSquenceNosPipeIdx[idx]=(short)pipeIdx;
			copyDataBlock(sourcePipe, pipeIdx, peekMsgId, 
					myPipeIdx, sequenceNo,
					channelId, true);
		} else {			        			
			if (expectedPipe ==(short)pipeIdx) {
				copyDataBlock(sourcePipe, pipeIdx, peekMsgId, 
						myPipeIdx, sequenceNo,
						channelId, false);
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


	private void copyDataBlock(final Pipe<ServerResponseSchema> sourcePipe, int pipeIdx, int peekMsgId,
			int myPipeIdx, int sequenceNo, long channelId, boolean beginningOfResponse) {
		
		failureIterations = -1;//clears the flag to let hang detector know we are ok.			        		

		Pipe<NetPayloadSchema> outPipe = outgoingPipes[myPipeIdx];
		
		assert(recordInputs(channelId, sequenceNo, pipeIdx));			        		
		if (!isTLS) {
		} else {
			finishHandshake(outPipe, channelId);
		}
		assert(Pipe.bytesReadBase(sourcePipe)>=0);				
		copyDataBlock(sourcePipe, peekMsgId, myPipeIdx, sequenceNo, channelId, beginningOfResponse);							
		assert(Pipe.bytesReadBase(sourcePipe)>=0);
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

	private void copyDataBlock(final Pipe<ServerResponseSchema> input, int peekMsgId,
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
		
	    //most common case by far so we put it first
	    if (ServerResponseSchema.MSG_TOCHANNEL_100 == activeMessageId ) {
	    	 	    	
	    	 if (showCounts) {
	    		System.out.println(++countOfMessages +" start of block "+(++countOfBlocks));
	    	 }
	    	 publishDataBlock(input, myPipeIdx, sequenceNo, channelId2, beginningOfResponse);
	    	
	    } else {
	    	
	    	//TOOD: can we combine multiple message into the same  block going out to the same destination????
	    	throw new UnsupportedOperationException("not yet implemented "+activeMessageId);
	    	
	    } 
	
		assert(Pipe.bytesReadBase(input)>=0);
	}


	private void publishDataBlock(final Pipe<ServerResponseSchema> input,
			                      int myPipeIdx, 
			                      int sequenceNo,
			                      long channelId, 
			                      boolean beginningOfResponse) {
				 
		
		 Pipe<NetPayloadSchema> output = outgoingPipes[myPipeIdx];
			
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
		 int meta = Pipe.takeByteArrayMetaData(input); //for string and byte array
		 int len = Pipe.takeByteArrayLength(input);
		
		 int requestContext = Pipe.takeInt(input); //high 1 upgrade, 1 close low 20 target pipe	                     
		 
		 
		 final int blobMask = Pipe.blobMask(input);
		 byte[] blob = Pipe.byteBackingArray(meta, input);
		 final int bytePosition = Pipe.bytePosition(meta, input, len); //also move the position forward
		
		 BaseConnection con = socketHolder.get(channelId);
		 ChannelReaderController connectionDataReader = null;
		 long arrivalTime = -1;
		 long businessTime = -1;
		 int routeId = -1;
		 
		 if (null!=con) {
			 channelId = con.id;
			 connectionDataReader = con.connectionDataReader;
			 ChannelReader reader = connectionDataReader.beginRead();
			 if (null!=reader && conStruct!=null) {
				 assert(reader.isStructured()) : "connection data reader must hold structured data";
				 arrivalTime = reader.structured().readLong(conStruct.arrivalTimeFieldId);
				 businessTime = reader.structured().readLong(conStruct.businessStartTime);
				 int origCon = reader.structured().readInt(conStruct.contextFieldId);				 
				 routeId = reader.structured().readInt(conStruct.routeIdFieldId);
				 			 				 
				 requestContext |= origCon;
				 

				 
				 if (beginningOfResponse) {
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
				 }
			 } else {
				 //warning this is an odd case which happens in telemetry.
				 //why is there no context stored.
			 }
		 } else {			 
		 }
		 if (len>0) {
			 DataOutputBlobWriter.write(outputStream, blob, bytePosition, len, blobMask);
		 }

		 
		 int temp = Pipe.bytesReadBase(input);
		 Pipe.confirmLowLevelRead(input, SIZE_OF_TO_CHNL);	 
		 Pipe.releaseReadLock(input);
	
		 boolean finishedFullReponse = true;//only false when we run out of room...
		 int y = 0;
		 //If a response was sent as multiple parts all part of the same sequence number then we roll them up as a single write when possible.
		 while ( Pipe.peekMsg(input, ServerResponseSchema.MSG_TOCHANNEL_100) 
			 	 && Pipe.peekInt(input, 0xFF&ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_SEQUENCENO_23) == expSeq 
			 	 && Pipe.peekLong(input, 0xFF&ServerResponseSchema.MSG_TOCHANNEL_100_FIELD_CHANNELID_21) == channelId 
			     && (finishedFullReponse =((len+Pipe.peekInt(input, PAYLOAD_LENGTH_IDX))<output.maxVarLen)) ) {
					 //this is still part of the current response so combine them together
					
			 		y++;
			 		
			 		 if (showCounts) {
			 			 System.out.println(++countOfMessages +" combined message ");
			 		 }
			 		 
					 int msgId = Pipe.takeMsgIdx(input); //msgIdx
					 long conId = Pipe.takeLong(input); //connectionId;
					 int seq = Pipe.takeInt(input);
					
					 assert(ServerResponseSchema.MSG_TOCHANNEL_100 == msgId);
					 assert(channelId == conId);
					 assert(seq == expSeq);
					 
					 
					 int meta2 = Pipe.takeByteArrayMetaData(input); //for string and byte array
					 int len2 = Pipe.takeByteArrayLength(input);
					 len+=len2;//keep running count so we can sure not to overflow the output

					 int bytePosition2 = Pipe.bytePosition(meta2, input, len2); //move the byte pointer forward
					 
					 //logger.info("read position {}",bytePosition2);
					 
					 DataOutputBlobWriter.write(outputStream, blob, bytePosition2, len2, blobMask);
					 	
					 //note: these masks are or'd with the previous
					 //upgrade, end-response, and close connection apply the same 
					 requestContext |= Pipe.takeInt(input); 
										 
					 Pipe.confirmLowLevelRead(input, SIZE_OF_TO_CHNL);	 
					 
					 Pipe.releaseReadLock(input);
					 
		 }
		 assert(Pipe.bytesReadBase(input)>=0);
		 
		 if (finishedFullReponse) {
			 //nothing after this point needs this data so it is abandoned.
			 if (null!=connectionDataReader) {
				 connectionDataReader.commitRead();
			 }
			 
			 long limitSLA = -1;
			 if (routeId<routeSLA.length && routeId>=0) {
				 limitSLA = routeSLA[routeId];				 
			 }
			 
			 if ((null!=log) || (limitSLA>0)) {
				 
				 long now = System.nanoTime();
				 long businessDuration = now-businessTime;
				 
				 if (businessDuration>limitSLA) {
					 //send the routeId to those listening that we have a violation
					 
					 //TODO: publish if pipe is found..
					 
					 
				 }
				 				 
				 //if we have a log pipe then log these events
				 Pipe<HTTPLogResponseSchema> outLog = log;
				 if (null!=outLog) {
					 //the log max var len may not be large enough for the entire payload
					 //so this may need to write multiple messages to capture all the data.
					 
					 DataOutputBlobWriter<?> logStr = Pipe.openOutputStream(outLog);				 
					 Pipe.outputStream(output).replicate(logStr);
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
						 
						 Pipe.addLongValue(businessDuration, outLog);
						 Pipe.confirmLowLevelWrite(outLog, size);
						 Pipe.publishWrites(outLog);
					 } while (writeLength>0);
					 
				 }
			 }			 
			 
		 } else {
			 if (null!=connectionDataReader) {
				 connectionDataReader.rollback();
			 }
		 }

         
		 if (showUTF8Sent) {
			 Pipe.outputStream(output).debugAsUTF8();
		 }
		 
		 writeToNextStage(output, myPipeIdx, channelId, requestContext); 
		 	 
		 
		 assert(Pipe.bytesReadBase(input)>=0);
		 assert(0==Pipe.releasePendingByteCount(input));
		 assert(Pipe.bytesReadBase(input)>=0);

	}

	private int writeToNextStage(Pipe<NetPayloadSchema> output, int myPipeIdx,
			final long channelId, int requestContext) {
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
		 Pipe.publishWrites(output);
		 
		 if (0 != (END_RESPONSE_MASK & requestContext)) {
		    
			//we have finished all the chunks for this request so the sequence number will now go up by one	
		 	int idx = (int)(channelId & channelBitsMask);
		 	//logger.info("detected end and incremented sequence number {}",expectedSquenceNos[idx]);
		 	
		 	//NOTE: it is critical that the only time we inc is when the end of the response is reached.
			expectedSquenceNos[idx]++;
		 	expectedSquenceNosPipeIdx[idx] = (short)-1;//clear the assumed pipe
		 	
		 	//logger.info("increment expected for chnl {}  to value {} len {}",channelId, expectedSquenceNos[(int)(channelId & coordinator.channelBitsMask)], len);
		 	

		 } 
		                     
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
