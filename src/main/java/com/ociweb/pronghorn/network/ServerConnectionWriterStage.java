package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ServiceObjectHolder;

public class ServerConnectionWriterStage extends PronghornStage {
    
    private static Logger logger = LoggerFactory.getLogger(ServerConnectionWriterStage.class);
    private ServiceObjectHolder<SocketChannel> socketHolder;    
    
    private final Pipe<ServerResponseSchema>[] dataToSend;

    
    private final ServerCoordinator coordinator;
    private final int pipeIdx;
    
    private SocketChannel  writeToChannel;
    private boolean        writeDone = true;
    private boolean        keepOpenAfterWrite = true;
    private int            activePipe = 0;
    private int            activeMessageId;
    private int[]          expectedSquenceNos;
    
    
    public final static int UPGRADE_TARGET_PIPE_MASK     = (1<<21)-1;
 
    public final static int UPGRADE_CONNECTION_SHIFT     = 31;    
    public final static int UPGRADE_MASK                 = 1<<UPGRADE_CONNECTION_SHIFT;
    
    public final static int CLOSE_CONNECTION_SHIFT       = 30;
    public final static int CLOSE_CONNECTION_MASK        = 1<<CLOSE_CONNECTION_SHIFT;
    
    public final static int END_RESPONSE_SHIFT           = 29;//for multi message send this high bit marks the end
    public final static int END_RESPONSE_MASK            = 1<<END_RESPONSE_SHIFT;
    
    public final static int INCOMPLETE_RESPONSE_SHIFT    = 28;
    public final static int INCOMPLETE_RESPONSE_MASK     = 1<<INCOMPLETE_RESPONSE_SHIFT;
    
    
    /**
     * 
     * Writes pay-load back to the appropriate channel based on the channelId in the message.
     * 
     * + ServerResponseSchema is custom to this stage and supports all the features here
     * + Has support for upgrade redirect pipe change (Module can clear bit to prevent this if needed)
     * + Has support for closing connection after write as needed for HTTP 1.1 and 0.0
     * 
     * 
     * + Will Have support for writing same pay-load to multiple channels (subscriptions)
     * + Will Have support for order enforcement and pipelined requests
     * 
     * 
     * @param graphManager
     * @param dataToSend
     * @param coordinator
     * @param pipeIdx
     */
    public ServerConnectionWriterStage(GraphManager graphManager, Pipe<ServerResponseSchema>[] dataToSend, ServerCoordinator coordinator, int pipeIdx) {
        super(graphManager, dataToSend, NONE);
        this.coordinator = coordinator;
        this.pipeIdx = pipeIdx;
        this.dataToSend = dataToSend;
    }
    
    @Override
    public void startup() {
                
        socketHolder = ServerCoordinator.getSocketChannelHolder(coordinator, pipeIdx);
        
        expectedSquenceNos = new int[coordinator.channelBitsSize];
    }
    
    @Override
    public void run() {
       
        //NOTE: TODO: BBB For the websockets,  add subscription support, eg N channels get the same message (should be in config already)
             //need list of channels off the one Id  requires new schema update

        boolean done = publish(writeToChannel);

        
        int c = dataToSend.length;
      //  logger.info("done {} {}",done,c);
        while (done && --c>= 0) {
            while (done && Pipe.hasContentToRead(dataToSend[activePipe])) {
                
            	//logger.info("sendng new content");
            	
                //peek to see if the next message should be blocked, eg out of order, if so skip to the next pipe
                int peekId = Pipe.peekInt(dataToSend[activePipe], 0);
                if (peekId>=0) {
                    long channelId = Pipe.peekLong(dataToSend[activePipe], 1);
                    int sequenceNo = Pipe.peekInt(dataToSend[activePipe], 3);
                    int pos = (int)(channelId & coordinator.channelBitsMask);
                    
                    int expected = expectedSquenceNos[pos];                
                    
                    //read the next non-blocked pipe, sequenceNo is never reset to zero
                    //every number is used even if there is an exception upon write.
                    boolean isBlocked = sequenceNo!=expected; 
                    if (isBlocked) {
                    	
                    	logger.info("unable to send {} {} blocked", sequenceNo, expected);
                    	
                        nextPipe();
                        continue;
                    }
               //     expectedSquenceNos[(int)(channelId & coordinator.channelBitsMask)] = expected+1 END_RESPONSE_MASK; //only add bit when we reach the end!!!!!
                    
                }
                if ((activeMessageId = Pipe.takeMsgIdx(dataToSend[activePipe])) < 0) {
                    requestShutdown();
                    return;
                }
                
                loadPayloadForXmit();
    
                done = publish(writeToChannel);
            }

            if (done) {
                nextPipe();
            }
        }
  
    }

    private void nextPipe() {
        if (--activePipe < 0) {
            activePipe = dataToSend.length-1;
        }
    }

    

    private void loadPayloadForXmit() {
        
        Pipe<ServerResponseSchema> pipe = dataToSend[activePipe];
        final long channelId = Pipe.takeLong(pipe);
        writeToChannel = socketHolder.get(channelId); //ChannelId or SubscriptionId
        
        Pipe.takeValue(pipe); //sequence number
        //we have already selected and confirmed this above, so nothing to do this value        
        
        //byteVector is payload
        int meta = Pipe.takeRingByteMetaData(pipe); //for string and byte array
        int len = Pipe.takeRingByteLen(pipe);

        int requestContext = Pipe.takeValue(pipe); //high 1 upgrade, 1 close low 20 target pipe
        if (0 != (UPGRADE_MASK & requestContext)) {
            //set the pipe for any further communications
            ServerCoordinator.setTargetUpgradePipeIdx(coordinator, pipeIdx, channelId, UPGRADE_TARGET_PIPE_MASK & requestContext);                        
        }
        keepOpenAfterWrite =  (0 == (CLOSE_CONNECTION_MASK  & requestContext));
                
        if (0 != (END_RESPONSE_MASK & requestContext)) {
        	
        	expectedSquenceNos[(int)(channelId & coordinator.channelBitsMask)]++;
        	
        	System.out.println("found end of response, increment sequence for "+(channelId & coordinator.channelBitsMask)+" next expected "+expectedSquenceNos[(int)(channelId & coordinator.channelBitsMask)]);
        	
        }
                
//        byte[] t1 = Pipe.byteBackingArray(meta, pipe);
//        int t2 = Pipe.blobMask(pipe);
//        int t3 = Pipe.bytePosition(meta, pipe, len);
//        
//        Appendables.appendUTF8(System.out, t1, t3, len, t2);
        
        //BROKEN RESPONSE.
//        0x2 200 OK
//        Server: Pronghorn
//        ETag:217721
//        Content-Type: HTTP/1.1
//        Content-Length: text/html
//
//        Connection: open
        
        
        writeBuffs= Pipe.wrappedReadingBuffers(pipe, meta, len);
        writeDone = false;
                
    }
    ByteBuffer[] writeBuffs;

    private boolean publish(SocketChannel channel) {
        if (writeDone) {
        	///logger.info("A");
            //do nothing if already done 
            return true;
        } else {            
            if (null!=channel && channel.isOpen()) { 
            	//logger.info("B");
            	return writeToChannel(channel);                
            } else {          
            	//logger.info("C");
                //if channel is closed drop the data 
                markDoneAndRelease();  
                return true;
            }
        }
    }

    private boolean writeToChannel(SocketChannel channel) {
        
        try {                
            
        	channel.write(writeBuffs);
                       
        	if (writeBuffs[0].hasRemaining() || writeBuffs[1].hasRemaining()) {
        		logger.warn("no room to write to channel");
        		return false;        		
        	} else {
        	    markDoneAndRelease();
        	    if (!keepOpenAfterWrite) {
        	    	closeChannel(channel);
        	    }
        	    return true;
        	}
        } catch (IOException e) {
            //unable to write to this socket, treat as closed
            markDoneAndRelease();
            logger.warn("unable to write to channel",e);
            closeChannel(channel);
            return true;
        }
    }

    private void closeChannel(SocketChannel channel) {
        try {
            channel.close();
        } catch (IOException e1) {
            logger.warn("unable co close channel",e1);
        }
    }

    private void markDoneAndRelease() {
        writeDone = true;        
        Pipe<ServerResponseSchema> pipe = dataToSend[activePipe];
        Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, activeMessageId));
        Pipe.releaseReadLock(pipe);
        
        //logger.info("done and release message {}",pipe);
    }

    
}
