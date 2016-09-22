package com.ociweb.pronghorn.stage.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.network.schema.ServerRequestSchema;
import com.ociweb.pronghorn.stage.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ServiceObjectHolder;

public class ServerConnectionReaderStage extends PronghornStage {
    
    public static final Logger logger = LoggerFactory.getLogger(ServerConnectionReaderStage.class);
    
    private final Pipe<ServerRequestSchema>[] output;

    private final ServerCoordinator coordinator;
    private final int pipeIdx;
    
    private Selector selector;
    private int selectionKeysAllowedToWait = 0;
        
    private Iterator<SelectionKey>    keyIterator;
    private SocketChannel             socketChannel;
    private long                      channelId;
    private Pipe<ServerRequestSchema> targetPipe;

    private ServiceObjectHolder<SocketChannel> holder;
    
    
    public ServerConnectionReaderStage(GraphManager graphManager, Pipe<ServerRequestSchema>[] output, ServerCoordinator coordinator, int pipeIdx) {
        super(graphManager, NONE, output);
        this.coordinator = coordinator;
        this.pipeIdx = pipeIdx;
        this.output = output;
        GraphManager.addNota(graphManager,GraphManager.PRODUCER, GraphManager.PRODUCER, this);
     //   GraphManager.addNota(graphManager,GraphManager.SCHEDULE_RATE, 300_000, this);
        
    }

    @Override
    public void startup() {

        holder = ServerCoordinator.newSocketChannelHolder(coordinator, pipeIdx);
                
        try {
            coordinator.registerSelector(pipeIdx, selector = Selector.open());
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        keyIterator = selector.selectedKeys().iterator();
        
    }
    
    @Override
    public void shutdown() {
        System.out.println("finsihed reading");
    }

    
    @Override
    public void run() {
//        
//        try {
//            Thread.sleep(3);
//        } catch (InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
        
        ////////////////////////////////////////
        ///Read from socket
        ////////////////////////////////////////

        if (hasNewDataToRead()) {
            
            //System.out.println("found new data to read");
            //TODO: when to shutdown??

            boolean readAll = true;
            
            if (readAll) {
                //we know that there is an interesting (non zero positive) number of keys waiting.
                if (null == socketChannel) {
                    keyIterator = selector.selectedKeys().iterator();   
                } else {
                    //continue reading open socket we did not finish last time due to pipe space
                    if (pumpByteChannelIntoPipe(socketChannel, channelId, targetPipe)) {//consumes from channel until it has no more or pipe has no more room
                        keyIterator.remove(); 
                        socketChannel = null;
                    } else {
                        return;//still no room in pipe;
                    }
                }
            } else {
                //TODO: test to see if this inerleaves the way I think it might, if it does this will be better for very large upload posts.
                keyIterator = selector.selectedKeys().iterator();
            }
            
            while (keyIterator.hasNext()) {                
                
                SelectionKey selection = keyIterator.next();
                assert(0 != (SelectionKey.OP_READ & selection.readyOps())) : "only expected read"; 
                socketChannel = (SocketChannel)selection.channel();
           
                
                //get the context object so we know what the channel identifier is
                channelId = ((ConnectionContext)selection.attachment()).channelId;
                
                targetPipe = output[ServerCoordinator.getTargetUpgradePipeIdx(coordinator, pipeIdx, channelId)];
                
                //TODO: note above that every channel gets a pipe, this should be changed so we have a small fixed number of pipes.
                
                
                if (pumpByteChannelIntoPipe(socketChannel, channelId, targetPipe)) {//consumes from channel until it has no more or pipe has no more room
                    keyIterator.remove();
                    socketChannel = null;
                } else {
                    break;//no room in pipe;
                }
            }
        }
    }

    private boolean hasNewDataToRead() {        
        try {
            return null!=socketChannel || selector.selectNow() > selectionKeysAllowedToWait;
        } catch (IOException e) {
            logger.error("unexpected shutdown, Selector for this group of connections has crashed with ",e);
            int i = output.length;
            while (--i >= 0) {
                Pipe.publishEOF(output[i]);                
            }
            requestShutdown();
            return false;
        }
    }
    
    //returns true if all the data for this chanel has been consumed
    public boolean pumpByteChannelIntoPipe(ReadableByteChannel sourceChannel, long channelId, Pipe<ServerRequestSchema> targetPipe) {
        
        int runningBlobPosition = Pipe.getBlobWorkingHeadPosition(targetPipe);
        
      //  System.out.println();
        
        //keep appending messages until the channel is empty or the pipe is full
        while (Pipe.hasRoomForWrite(targetPipe)) {          

            try {
                
                
                int len;//if data is read then we build a record around it
                //NOTE: the byte buffer is no longer than the valid maximum length but may be shorter based on end of wrap arround
                ByteBuffer[] b = Pipe.wrappedWritingBuffers(runningBlobPosition, targetPipe);
         //       System.out.println("rem:"+b.remaining()+"  "+b.capacity()+"   "+b.position()+"     "+b.limit());
                if ((len = sourceChannel.read(b[0])) > 0) { //NOTE: failure on read does NOT impact pipe in any way.
                    if (b[0].remaining()==0) {
                        int temp = sourceChannel.read(b[1]);
                        if (temp>0) {
                            len+=temp;
                        }
                    }
                    
                    publishData(targetPipe, channelId, runningBlobPosition, len);    
                    runningBlobPosition += len;
       //             System.out.println("reading "+len);
                } else {
                    //if nothing was read then the channel is empty
                    return true;
                }
            } catch (IOException e) {
                    recordErrorAndClose(sourceChannel, e);
                     return true;//remove this one its bad. 
            }
        }
        return false;//stopped because there was no room in the pipe
    }

    private void recordErrorAndClose(ReadableByteChannel sourceChannel, IOException e) {
        logger.error("unable to read",e);
          //may have been closed while reading so stop
          if (null!=sourceChannel) {
              try {
                  sourceChannel.close();
               } catch (IOException e1) {
                   logger.warn("unable to close channel",e1);
               }
              
          }
    }

    private void publishData(Pipe<ServerRequestSchema> targetPipe, long channelId, int originalBlobPosition, int len) {

        
        int size = Pipe.addMsgIdx(targetPipe,ServerRequestSchema.MSG_FROMCHANNEL_100);               
        Pipe.addLongValue(channelId, targetPipe);  
        
        Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, len, targetPipe);  
        
        //all breaks are detected by the router not here
        //(section 4.1 of RFC 2616) end of header is \r\n\r\n but some may only send \n\n
        //
   
        Pipe.confirmLowLevelWrite(targetPipe, size);
        Pipe.publishWrites(targetPipe);
    }
    
}
