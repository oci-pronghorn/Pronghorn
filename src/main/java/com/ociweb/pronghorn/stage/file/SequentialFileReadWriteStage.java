package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;

import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.SequentialCtlSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialRespSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SequentialFileReadWriteStage extends PronghornStage {

    private static final int SIZE = RawDataSchema.FROM.fragDataSize[0];
        
    private static final Logger logger = LoggerFactory.getLogger(SequentialFileReadWriteStage.class);
    
    private static final byte MODE_WRITE = 0;
    private static final byte MODE_READ  = 1;
        
    private final int READ_CTL_REQUIRED_SIZE = Pipe.sizeOf(RawDataSchema.instance, 2*RawDataSchema.MSG_CHUNKEDSTREAM_1);
    private final Pipe<RawDataSchema>[] output;
    private final Pipe<RawDataSchema>[] input;
    private final Pipe<SequentialCtlSchema>[] control;
    private final Pipe<SequentialRespSchema>[] response;
    private final String[] paths;
    private Path[] path;
    private long[] idToWriteToFile;
    
    private FileChannel[] fileChannel;
    private int[] shutdownInProgress; // 0 not, 1 going, 3 sent
    private int shutdownCount;
    
    private boolean[] releaseRead; //all false for initial state
    private ByteBuffer[] buffA;
    private ByteBuffer[] buffB;
    private byte[] mode; //all WRITE ZERO for initial state

    private FileSystemProvider provider;
    private FileSystem fileSystem;
    private Set<OpenOption> options;

    public SequentialFileReadWriteStage(GraphManager graphManager,
	    						 Pipe<SequentialCtlSchema>[] control,
	    						 Pipe<SequentialRespSchema>[] response,
	    						 Pipe<RawDataSchema>[] input,
	    		                 Pipe<RawDataSchema>[] output, 
	    		                 String[] paths) {
    	
        super(graphManager, join(control, input), join(response, output));
        
        this.paths = paths;
        this.output = output;
        this.input = input;
        this.control = control;
        this.response = response;
        
        assert(paths.length == output.length);
        assert(input.length == output.length);
        assert(input.length == control.length);
        assert(response.length == control.length);
    
    }

    public String toString() {
    	String parent = super.toString();
    	
    	//invariant
    	for(int p=0;p<path.length;p++) {
    		parent += ("\n"+path[p]);    		
    	}
    	
    	return parent;
    }
    
    
    @Override
    public void startup() {

    	this.idToWriteToFile = new long[output.length];//store ID to be written and then acked
    	Arrays.fill(this.idToWriteToFile, -1);
    	
        this.releaseRead = new boolean[output.length]; //all false for initial state
        this.buffA = new ByteBuffer[output.length];
        this.buffB = new ByteBuffer[output.length];
    	this.mode = new byte[output.length];
    	this.shutdownInProgress = new int[output.length];
        this.shutdownCount = output.length; //shuts down at zero
    	
    	this.fileSystem = FileSystems.getDefault();
        this.provider = fileSystem.provider();
        
        this.options = new HashSet<OpenOption>();
        this.options.add(StandardOpenOption.SYNC);
        this.options.add(StandardOpenOption.CREATE);
       	this.options.add(StandardOpenOption.WRITE);
        this.options.add(StandardOpenOption.READ);
                
        try {
        	int i = paths.length;
        	this.path = new Path[i];
        	this.fileChannel = new FileChannel[i];
        	while (--i>=0) {
        		this.path[i] = this.fileSystem.getPath(this.paths[i]);
				this.fileChannel[i] = this.provider.newFileChannel(path[i], this.options);
				this.fileChannel[i].position(this.fileChannel[i].size());
        	}
        } catch (IOException e) {
           throw new RuntimeException(e);
        } 
        
        //provider.readAttributes(path, attributes, options)
        //fileSystem.getPath("")
    }

    @Override
    public void shutdown() {
    	//logger.info("shutdown");
    }

    
    @Override
    public void run() {
    	assert(null!=buffA);
    	boolean didWork;
    	
    	do {
    		didWork = false;
    	
	    	int i = output.length;
	    	while(--i>=0) {
	    		
	    		if (shutdownInProgress[i]>0) {
	    				    		
	    			if (shutdownInProgress[i] == 1 &&
	    			    Pipe.hasRoomForWrite(output[i], Pipe.EOF_SIZE) && 
	    			    Pipe.hasRoomForWrite(response[i], Pipe.EOF_SIZE)) {
	    				
	    				Pipe.publishEOF(response[i]);
	    				Pipe.publishEOF(output[i]);
	    				
	    				try {
	            			if (null != fileChannel[i]) {
	            				fileChannel[i].close();
	            			}        			
	            		} catch (IOException e) {
	            			e.printStackTrace();
	            			throw new RuntimeException(e);
	            		}	    				
	    				
	    				if (--shutdownCount == 0) {
	    					//all files are done so shutdown
	    					requestShutdown();
	    				}
	    				shutdownInProgress[i] = 3;//only send EOF once
	    			}
	    		} else {
	    		
		    		//each of the possible activities will only require one response
		    		if (Pipe.hasRoomForWrite(response[i])) {
		    			if (readControl(i)) { //readControl may write to response for meta single msg
			    	   
		    				//only continue now if we did not consume the known available space on response
				    		if (MODE_WRITE == mode[i]) {
				    			didWork |= writeProcessing(i); //may write to response upon ack of each block single msg with block counts	    			
				    		} else {
				    			didWork |= readProcessing(i); //may write to response upon end of file read single msg
				    		}
			    		} else {
			    			didWork = true;
			    		}			    		
		    		}
	    		}
	    	}
	    	
    	} while (didWork);
    }

	private boolean readControl(int idx) {
		Pipe<SequentialCtlSchema> localControl = control[idx];
		boolean continueWithIdx = true;
		/////////
		//do not use while, commands are rare and we should only do 1
		//////////
		

		if ((-1==idToWriteToFile[idx]) 
				&& Pipe.hasContentToRead(localControl)
				&& Pipe.hasRoomForWrite(output[idx], READ_CTL_REQUIRED_SIZE)
				) {
		    int msgIdx = Pipe.takeMsgIdx(localControl);
		    		
		    switch(msgIdx) {
		    
		        case SequentialCtlSchema.MSG_REPLAY_1:
		        	//switch to read mode if not already in read mode
		        	//set position to beginning for reading
		        	if (MODE_WRITE == mode[idx]) {
		        		mode[idx] = MODE_READ;
		        		positionToBeginning(idx);
		        		//the mode will switch back to write up on reaching the end of the file.
		        	} else {
		        		logger.info("requested switch to read/replay but already in that mode");
		        	}	
				break;
		        case SequentialCtlSchema.MSG_CLEAR_2:
		        	//if in write mode set position to zero
		        	
		        	if (MODE_WRITE == mode[idx]) {
		        		//due to the above logic clear is only called after the write pipe is empty.
		        		positionToBeginning(idx);
		        		//no more data should be written until this gets consumed
		        		SequentialRespSchema.publishClearAck(response[idx]);		        		
		        	} else {
		        		logger.info("error, clear called while in read mode");
		        	}
		        	
		        	
				break;
		        case SequentialCtlSchema.MSG_METAREQUEST_3:

					try {
						BasicFileAttributes readAttributes = this.provider.readAttributes(path[idx], BasicFileAttributes.class);
						
						SequentialRespSchema.publishMetaResponse(
								response[idx], 
								readAttributes.size(), 
								readAttributes.lastModifiedTime().toMillis());
						
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
		        	
					//send mete data but also break out of above since we took the message
		        	continueWithIdx = false;
				break;
		        case SequentialCtlSchema.MSG_IDTOSAVE_4:
		        	idToWriteToFile[idx] = Pipe.takeLong(localControl);
		        	//logger.info("new block to be saved {} ", idToWriteToFile[idx]);
		        break;
		        case -1:
		        	logger.trace("control request for shutting down file {} ",idx);
		        	shutdownInProgress[idx] |= 1;
		        break;
		    }
		    
		    Pipe.confirmLowLevelRead(localControl, Pipe.sizeOf(localControl, msgIdx));
		    Pipe.releaseReadLock(localControl);
		}
		return continueWithIdx;
	}

	private void positionToBeginning(int idx) {
		try {
			fileChannel[idx].position(0);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		////////////////
		//not needed becaause decrypt starts at the beginning
		////////////////
//		//must send single to reset any decryption		
//		int size = Pipe.addMsgIdx(output[idx], RawDataSchema.MSG_CHUNKEDSTREAM_1);
//		Pipe.addNullByteArray(output[idx]);
//		Pipe.confirmLowLevelWrite(output[idx],size);
//		Pipe.publishWrites(output[idx]);
		
	}

	private boolean readProcessing(int idx) {
		////////////////////////////////////
		//reading from the file and write out to the stream
		////////////////////////////////////
		boolean didWork = false;
		Pipe<RawDataSchema> localOutput = output[idx];
		FileChannel localFileChannel = fileChannel[idx];
		
		while (Pipe.hasRoomForWrite(localOutput)) {
            //System.err.println("has room for write");
            int originalBlobPosition = Pipe.getWorkingBlobHeadPosition(localOutput);      
            try {            
                didWork = true;
                //attempt to read this many bytes but may read less
                long len = localFileChannel.read(Pipe.wrappedWritingBuffers(originalBlobPosition, localOutput));
                
                //A when we go into replay mode we send a -1 to trigger decrypt to cleanup
                //B we then send the data
                //C finally we send another -1 
  
                //We write the -1 len as a marker to those down stream that the end has been reached.
                int size = Pipe.addMsgIdx(localOutput, RawDataSchema.MSG_CHUNKEDSTREAM_1);
                Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int)len, localOutput);  
                Pipe.confirmLowLevelWrite(localOutput, size);
                Pipe.publishWrites(localOutput);    

               // logger.info("reading {} bytes from file {} into pipe {}",len,idx,localOutput);
                
                if (len<0) { 
                	//end of file reached so change to write                	
                	mode[idx] = MODE_WRITE;
                    return true;
                } 
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
        }
		return didWork;
	}

	private boolean writeProcessing(int idx) {
		//////////////////////////////////////
		//write from the stream to this file
		/////////////////////////////////////
				
		Pipe<RawDataSchema> localInput = input[idx];
		FileChannel localFileChannel = fileChannel[idx];
	
		do {
	        
	        if (null==buffA[idx] && 
	            null==buffB[idx]) {
	            //read the next block
	            
	            if (releaseRead[idx]) {
	                //only done after we have consumed the bytes
	                Pipe.confirmLowLevelRead(localInput, SIZE);
	                Pipe.releaseReadLock(localInput);
	                releaseRead[idx] = false;
	                
	                //was released so now send ack.
	                return ackFinishedWrite(idx);
	            }
	          //  logger.info("write processing for block id {} and file {} has content {} "
	          //  		,idToWriteToFile[idx],idx,Pipe.hasContentToRead(localInput));
	            
	            //TODO: accumulate multiple writes and do as 1 block, then be sure to return all the Acks
	            //      as we read them of the incomming pipe.
	            	
	            if ((-1!=idToWriteToFile[idx] && Pipe.hasContentToRead(localInput)) 
	            		|| Pipe.peekMsg(localInput, -1) ) {
	            	
	                int msgId      = Pipe.takeMsgIdx(localInput);   
	                if (msgId < 0) {
	                    Pipe.confirmLowLevelRead(localInput, Pipe.EOF_SIZE);
	                    Pipe.releaseReadLock(localInput);
	                    logger.trace("data driven shutting down of file {} ",idx);
	                    shutdownInProgress[idx] |= 1;
	                    return ackFinishedWrite(idx);
	                }
	                assert(0==msgId);
	                int meta = Pipe.takeRingByteMetaData(localInput); //for string and byte array
	                int len = Pipe.takeRingByteLen(localInput);
	
	                if (0==len) {
	                	logger.info("WARNING, file write of 0 bytes for the file {} has been done.",idx);
	                }
	 
	                if (len < 0) {
	                    Pipe.confirmLowLevelRead(localInput, SIZE);
	                    Pipe.releaseReadLock(localInput);
	                    //zero content to append (null) do not shut down
	                    return ackFinishedWrite(idx);
	                }	                
	                                                
	                releaseRead[idx] = true;
	                buffA[idx] = Pipe.wrappedBlobReadingRingA(localInput, meta, len);
	                buffB[idx] = Pipe.wrappedBlobReadingRingB(localInput, meta, len);
	                if (!buffB[idx].hasRemaining()) {
	                    buffB[idx] = null;
	                }
	                
	                
	            } else {
	            	return false;
	            }
	            
	        }
	        
	        
	        //we have existing data to be written
	        if (hasDataToWrite(idx)) {
	            try {
	                
	            	localFileChannel.write(buffA[idx]);
	                if (0==buffA[idx].remaining()) {
	                    buffA[idx] = null;
	                } else {   	//do not write B because we did not finish A
	                	return true;
	                }
	                
	            } catch (IOException e) {
	                throw new RuntimeException(e);
	            }
	        }
	        
	        if (null!=buffB[idx]) {
	            try {                
	            	localFileChannel.write(buffB[idx]);
	                if (0==buffB[idx].remaining()) {
	                    buffB[idx] = null;
	                }
	                
	            } catch (IOException e) {
	                throw new RuntimeException(e);
	            }
	        }
        
        } while (null == buffA[idx] && null == buffB[idx]);
		return false;
	}

	private boolean ackFinishedWrite(int idx) {
		if (-1 != idToWriteToFile[idx]) {
			//logger.info("finished write of block {} ", idToWriteToFile[idx]);
			SequentialRespSchema.publishWriteAck(response[idx], idToWriteToFile[idx]);
			idToWriteToFile[idx] = -1;
			return true;
		}
		return false;
	}


	private boolean hasDataToWrite(int idx) {
		return null!=buffA[idx];
	}


}
