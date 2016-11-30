package com.ociweb.pronghorn.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.NetParseAckSchema;
import com.ociweb.pronghorn.network.schema.NetPayloadSchema;
import com.ociweb.pronghorn.network.schema.ServerResponseSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ServiceObjectHolder;

public class ServerConnectionReaderStage extends PronghornStage {
    
    private final int messageType;

	public static final Logger logger = LoggerFactory.getLogger(ServerConnectionReaderStage.class);
    
    private final Pipe<NetPayloadSchema>[] output;
    private final Pipe<NetParseAckSchema>[] ack;
    private final ServerCoordinator coordinator;
    private final int groupIdx;
    
    private Selector selector;

    private int pendingSelections = 0;
    

    private ServiceObjectHolder<ServerConnection> holder;
    
    
    public ServerConnectionReaderStage(GraphManager graphManager, Pipe<NetParseAckSchema>[] ack, Pipe<NetPayloadSchema>[] output, ServerCoordinator coordinator, int pipeIdx, boolean encrypted) {
        super(graphManager, ack, output);
        this.coordinator = coordinator;
        this.groupIdx = pipeIdx;
        this.output = output;
        this.ack = ack;
        
        if (output.length<2) {
        	throw new RuntimeException("outputs count "+output.length);
        }
        this.messageType = encrypted ? NetPayloadSchema.MSG_ENCRYPTED_200 : NetPayloadSchema.MSG_PLAIN_210;
    }

    @Override
    public void startup() {

        holder = ServerCoordinator.newSocketChannelHolder(coordinator, groupIdx);
                
        try {
            coordinator.registerSelector(groupIdx, selector = Selector.open());
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        //logger.debug("selector is registered for pipe {}",pipeIdx);
        
    }
    
    @Override
    public void shutdown() {
        int i = output.length;
        while (--i >= 0) {
        	Pipe.spinBlockForRoom(output[i], Pipe.EOF_SIZE);
            Pipe.publishEOF(output[i]);                
        }
        logger.warn("server reader has shut down");
    }

    int found = 0;
    
    @Override
    public void run() {
        
    	releasePipesForUse();    	
    	
        ////////////////////////////////////////
        ///Read from socket
        ////////////////////////////////////////

        if (hasNewDataToRead()) {
        	
        	//logger.debug("found new data to read on "+groupIdx);
            
            Iterator<SelectionKey>  keyIterator = selector.selectedKeys().iterator();   
            
            while (keyIterator.hasNext()) {                
                
            	//logger.info("selector has data");
            	
                SelectionKey selection = keyIterator.next();
                assert(0 != (SelectionKey.OP_READ & selection.readyOps())) : "only expected read"; 
                SocketChannel socketChannel = (SocketChannel)selection.channel();
           
                //logger.info("is blocking {} open {} ", selection.channel().isBlocking(),socketChannel.isOpen());
                
                
                //get the context object so we know what the channel identifier is
                ConnectionContext connectionContext = (ConnectionContext)selection.attachment();                
				long channelId = connectionContext.getChannelId();
                				
				releasePipesForUse();
				int responsePipeLineIdx = coordinator.responsePipeLineIdx(channelId);
				
				/////////////////
				///ERROR: the high speed processing keeps the pipe held by the user so until it is done with data it will not be free
				//       but at the same time we ahve new handshakes that need to get in
				//////////   THE HANDSHAKES ARE STARVED OUT....
				
				
				
				
				if (-1 == responsePipeLineIdx) { //handshake is dropped by input buffer at these loads?
					Thread.yield();
					releasePipesForUse();
					responsePipeLineIdx = coordinator.responsePipeLineIdx(channelId);
					if (-1 == responsePipeLineIdx) {
//						if (found<30) {
//							logger.info("CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCc  server was not able to find free pipe for {} so try again soon", channelId);
//							found++;
//						}
		                keyIterator.remove();
		                pendingSelections--;   
						continue;
					}
				}
				
				Pipe<NetPayloadSchema> targetPipe = output[responsePipeLineIdx]; //TODO: add support for groupIdx here? each group needs its own pool....                
                if (!pumpByteChannelIntoPipe(socketChannel, channelId, targetPipe)) {//consumes from channel until it has no more or pipe has no more room
                	//end of stream
                	
                	logger.error("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX end of stream detected {} closing channel",channelId);
                	
                	try {
						socketChannel.close();
						selection.cancel();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
                }
                
                keyIterator.remove();
                pendingSelections--;             
                releasePipesForUse();
            }
        }
    }

	private void releasePipesForUse() {
		int i = ack.length;
		while (--i>=0) {
			Pipe<NetParseAckSchema> a = ack[i];
			while (Pipe.hasContentToRead(a)) {
	    						
	    		int id = Pipe.takeMsgIdx(a);
	    		if (id == NetParseAckSchema.MSG_PARSEACK_100) {
	    			long idToClear = Pipe.takeLong(a); //PipeReader.readLong(a, NetParseAckSchema.MSG_PARSEACK_100_FIELD_CONNECTIONID_1);
	    			long pos = Pipe.takeLong(a);//PipeReader.readLong(a, NetParseAckSchema.MSG_PARSEACK_100_FIELD_POSITION_2);
	    			
	    			int pipeIdx = coordinator.responsePipeLineIdx(idToClear);
	    			
	    			///////////////////////////////////////////////////
	    			//if sent tail matches the current head then this pipe has nothing in flight and can be re-assigned
	    			if (pipeIdx>=0 && Pipe.headPosition(output[pipeIdx]) == pos && Pipe.contentRemaining(output[pipeIdx])==0  ) {
	    				       //TODO: why is the pipe content check needed? seems reduntant but it is needed to pass tests.. was only needed after we added multiple unwrappers.
	    				coordinator.releaseResponsePipeLineIdx(idToClear);
	    			}    			
	    			Pipe.confirmLowLevelRead(a, Pipe.sizeOf(NetParseAckSchema.instance, NetParseAckSchema.MSG_PARSEACK_100));
	    		} else {
	    			assert(-1==id);
	    			Pipe.confirmLowLevelRead(a, Pipe.EOF_SIZE);
	    		}
	    		Pipe.releaseReadLock(a);	    		
	    	}
		}
	}

    private boolean hasNewDataToRead() {
    	
    	if (pendingSelections>0) {
    		return true;
    	}
    	
        try {        	        	
        	/////////////
        	//CAUTION - select now clears pevious count and only returns the additional I/O opeation counts which have become avail since the last time SelectNow was called
        	////////////        	
            return (pendingSelections=selector.selectNow()) > 0;
        } catch (IOException e) {
            logger.error("unexpected shutdown, Selector for this group of connections has crashed with ",e);
            requestShutdown();
            return false;
        }
    }
    
    //returns true if all the data for this chanel has been consumed
    public boolean pumpByteChannelIntoPipe(ReadableByteChannel sourceChannel, long channelId, Pipe<NetPayloadSchema> targetPipe) {
        
       
        //keep appending messages until the channel is empty or the pipe is full
        while (Pipe.hasRoomForWrite(targetPipe)) {          
        	//logger.info("pump one ");
            try {                
                
                int len;//if data is read then we build a record around it
                //NOTE: the byte buffer is no longer than the valid maximum length but may be shorter based on end of wrap arround
                ByteBuffer[] b = Pipe.wrappedWritingBuffers(Pipe.storeBlobWorkingHeadPosition(targetPipe), targetPipe);
                if ((len = sourceChannel.read(b[0])) > 0) { //NOTE: failure on read does NOT impact pipe in any way.
                	//if we filled the first buffer we may still have extra data for the second
                    int temp = 0;
                	if (b[0].remaining()==0) {
                        temp = sourceChannel.read(b[1]);
                        if (temp>0) {
                            len+=temp;
                        }
                    }
                    
                    publishData(targetPipe, channelId, len);    
                  
                    //logger.info("server published {} to unwrap for connection {} ",len,channelId);
                    
                    if (b[0].remaining()>0 || b[1].remaining()>0) {
                    	//copied all the data from the source channel
                    	//return temp!=-1;//TODO: testing, do not close but return true
                    	return true;
                    } else {
                    	if (temp==-1) {
                    		//return false;//TODO: testing, do not close but return true
                    		return true;
                    	}
                    	//logger.info("more data may exist, we did not have enought room in a single outgoing message.");
                    	
                    }
                    
                } else {
                	assert(b[0].remaining()>0) : "Should not be here if we have no room to write target";
                	//logger.info("written not published "+len+" space "+b[0].remaining());
                	
                	Pipe.unstoreBlobWorkingHeadPosition(targetPipe);//we did not use or need the writing buffers above.
                	//copied all the data from the source channel
                	return len!=-1;
                }
             
            } catch (IOException e) {
                    recordErrorAndClose(sourceChannel, e);
                    return false;
            }
        }
        return true;//stopped because there was no room in the pipe
    }

    private void recordErrorAndClose(ReadableByteChannel sourceChannel, IOException e) {
        //logger.error("unable to read",e);
          //may have been closed while reading so stop
          if (null!=sourceChannel) {
              try {
                  sourceChannel.close();
               } catch (IOException e1) {
                   logger.warn("unable to close channel",e1);
               }
              
          }
    }

    private void publishData(Pipe<NetPayloadSchema> targetPipe, long channelId, int len) {

        
        int size = Pipe.addMsgIdx(targetPipe,messageType);               
        Pipe.addLongValue(channelId, targetPipe);  

        if (NetPayloadSchema.MSG_PLAIN_210 == messageType) {
        	Pipe.addLongValue(Pipe.getWorkingTailPosition(targetPipe), targetPipe);
        }
        
        int originalBlobPosition =  Pipe.unstoreBlobWorkingHeadPosition(targetPipe);
       
        //logger.info("server got: "+Appendables.appendUTF8(new StringBuilder(), Pipe.blob(targetPipe), originalBlobPosition, len, Pipe.blobMask(targetPipe)));
  
//EXAMPLE REQUEST        
//        GET /index.html HTTP/1.1
//        Host: 127.0.0.1:8081
//        Connection: keep-alive
//        Cache-Control: max-age=0
//        Upgrade-Insecure-Requests: 1
//        User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/53.0.2785.143 Chrome/53.0.2785.143 Safari/537.36
//        Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8
//        DNT: 1
//        Accept-Encoding: gzip, deflate, sdch
//        Accept-Language: en-US,en;q=0.8
//        Cookie: shellInABox=3:101010


        
        Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, len, targetPipe);  
        
        //all breaks are detected by the router not here
        //(section 4.1 of RFC 2616) end of header is \r\n\r\n but some may only send \n\n
        //
   
        Pipe.confirmLowLevelWrite(targetPipe, size);
        Pipe.publishWrites(targetPipe);
        
    }
    
}
