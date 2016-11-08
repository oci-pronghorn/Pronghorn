package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;


//consumes the sequence number in order and hold a pool entry for this connection
//sends the data in order to the right pool entry for encryption to be applied down stream.
//TODO: should add feature of subscriptions here due to it being before the encryption stage.
public class WrapSupervisorStage extends PronghornStage { //AKA re-ordering stage
    
    private static Logger logger = LoggerFactory.getLogger(WrapSupervisorStage.class);

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
    
    public final int poolMod;
    public final int maxOuputSize;
    
    
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
    public WrapSupervisorStage(GraphManager graphManager, Pipe<ServerResponseSchema>[] inputPipes, Pipe<NetPayloadSchema>[] outgoingPipes, ServerCoordinator coordinator) {
        super(graphManager, inputPipes, outgoingPipes);
      
        this.dataToSend = inputPipes;
        this.outgoingPipes = outgoingPipes;
        this.coordinator = coordinator;
        
        this.poolMod = outgoingPipes.length;
        
        if (minVarLength(outgoingPipes) < maxVarLength(inputPipes)) {
        	throw new UnsupportedOperationException("All output pipes must support variable length fields equal to or larger than all input pipes");
        }
        
        this.maxOuputSize = Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_PLAIN_210) +
        								Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_UPGRADE_207) +            
        								Pipe.sizeOf(NetPayloadSchema.instance, NetPayloadSchema.MSG_DISCONNECT_203);
    }


	@Override
    public void startup() {                
        expectedSquenceNos = new int[coordinator.channelBitsSize];
    }
	
	@Override
	public void shutdown() {
		int i = outgoingPipes.length;
		while (--i>=0) {
			Pipe.spinBlockForRoom(outgoingPipes[i], Pipe.EOF_SIZE);  //TODO: this is a re-occuring pattern perhaps this belongs in the base class since every actor does it.
			Pipe.publishEOF(outgoingPipes[i]);
		}
	}
    
    @Override
    public void run() {

        int c = dataToSend.length;     
        
        while (--c>= 0) {
        	Pipe<ServerResponseSchema> sourcePipe = dataToSend[c];
            while (Pipe.hasContentToRead(sourcePipe)) {
                
                //peek to see if the next message should be blocked, eg out of order, if so skip to the next pipe
                int peekMsgId = Pipe.peekInt(sourcePipe, 0);
                Pipe<NetPayloadSchema> myPipe = null;
                int sequenceNo = 0;
                if (peekMsgId>=0) {
                    long channelId = Pipe.peekLong(sourcePipe, 1);
                    sequenceNo = Pipe.peekInt(sourcePipe,  3);
                     
                    
                    int expected = expectedSquenceNos[(int)(channelId & coordinator.channelBitsMask)];                
                    
                    
                    //read the next non-blocked pipe, sequenceNo is never reset to zero
                    //every number is used even if there is an exception upon write.
                    boolean isBlocked = false;//sequenceNo!=expected; 
                    
                    if (isBlocked) {
                    	System.out.println("in use connection "+(int)(channelId & coordinator.channelBitsMask)+" must match expected "+expected+" and sequenceNo "+sequenceNo);
                    	
                    	logger.info("unable to send {} {} blocked", sequenceNo, expected);
                    	
                    	break;
                    }
                    //////////////////////
                    //not blocked by sequence order so we must check if we are blocked by having write room.
                    //////////////////////
                    
                    myPipe = outgoingPipes[(int)(channelId % poolMod)];
                    
                    if (!Pipe.hasRoomForWrite(myPipe, maxOuputSize)) {
                    //	logger.info("unable to send data, output pipe is full {}",myPipe);
                    	break;
                    }
                } else {
                	Pipe.takeMsgIdx(sourcePipe);
                	Pipe.confirmLowLevelRead(sourcePipe, Pipe.EOF_SIZE);
                	Pipe.releaseReadLock(sourcePipe);
                	requestShutdown();
                	return;
                }
                
                ////////////////////////////////////////////////////
                //we now know that this work should be done and that there is room to put it out on the pipe
                //so do it already
                ////////////////////////////////////////////////////
                //the EOF message has already been taken so no need to check 
                //all remaning messages start with the connection id
                
                final int activeMessageId = Pipe.takeMsgIdx(sourcePipe);
                assert(peekMsgId == activeMessageId);
                final long channelId = Pipe.takeLong(sourcePipe);
                
                
                //most common case by far so we put it first
                if (ServerResponseSchema.MSG_TOCHANNEL_100 == activeMessageId ) {
                	                	             	  
                	 int expSeq = Pipe.takeInt(sourcePipe); //sequence number
                	 assert(sequenceNo == expSeq);
                	 
                     //byteVector is payload
                     int meta = Pipe.takeRingByteMetaData(sourcePipe); //for string and byte array
                     int len = Pipe.takeRingByteLen(sourcePipe);
                     int requestContext = Pipe.takeInt(sourcePipe); //high 1 upgrade, 1 close low 20 target pipe
                     
                     
                     /////////////
                     //if needed write out the upgrade message
                     ////////////
                     
                     if (0 != (UPGRADE_MASK & requestContext)) {
                    	 
                    	 //the next response should be routed to this new location
                    	 int upgSize = Pipe.addMsgIdx(myPipe, NetPayloadSchema.MSG_UPGRADE_207);
                    	 Pipe.addLongValue(channelId, myPipe);
                    	 Pipe.addIntValue(UPGRADE_TARGET_PIPE_MASK & requestContext, myPipe);;
                    	 Pipe.confirmLowLevelWrite(myPipe, upgSize);
                    	 Pipe.publishWrites(myPipe);
                    	 
                     }
                	                	
                     /////////////
                     //write out the content
                     /////////////
                                          
                     int plainSize = Pipe.addMsgIdx(myPipe, NetPayloadSchema.MSG_PLAIN_210);
                     Pipe.addLongValue(channelId, myPipe);
                     Pipe.addByteArrayWithMask(myPipe, Pipe.blobMask(sourcePipe), len, Pipe.blob(sourcePipe), Pipe.bytePosition(meta, sourcePipe, len));
                     Pipe.confirmLowLevelWrite(myPipe, plainSize);
                     Pipe.publishWrites(myPipe);
                     
                     if (0 != (END_RESPONSE_MASK & requestContext)) { 
                        //we have finished all the chunks for this request so the sequence number will now go up by one	
                     	expectedSquenceNos[(int)(channelId & coordinator.channelBitsMask)]++;                     	
                     }
                                         
                     
                     //////////////
                     //if needed write out the close connection message
                     //////////////
                     
                     if (0 != (CLOSE_CONNECTION_MASK & requestContext)) { 
                         
                    	 int disSize = Pipe.addMsgIdx(myPipe, NetPayloadSchema.MSG_DISCONNECT_203);
                    	 Pipe.addLongValue(channelId, myPipe);
                    	 Pipe.confirmLowLevelWrite(myPipe, disSize);
                    	 Pipe.publishWrites(myPipe);
                    	 
                     }                     
                     
                     Pipe.confirmLowLevelRead(sourcePipe, Pipe.sizeOf(sourcePipe, ServerResponseSchema.MSG_TOCHANNEL_100));
                     Pipe.releaseReadLock(sourcePipe);
                	
                	
                } else if (ServerResponseSchema.MSG_TOSUBSCRIPTION_200 == activeMessageId ) {
                	
                	throw new UnsupportedOperationException();
                	
                }    
            }
        }  
    }
    
}
