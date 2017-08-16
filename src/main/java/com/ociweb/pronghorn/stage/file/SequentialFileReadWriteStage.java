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
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileControlSchema;
import com.ociweb.pronghorn.stage.file.schema.SequentialFileResponseSchema;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class SequentialFileReadWriteStage extends PronghornStage {

    private static final int SIZE = RawDataSchema.FROM.fragDataSize[0];
        
    private static final Logger logger = LoggerFactory.getLogger(SequentialFileReadWriteStage.class);
    
    private static final byte MODE_WRITE = 0;
    private static final byte MODE_READ  = 1;
        
    
    private final Pipe<RawDataSchema>[] output;
    private final Pipe<RawDataSchema>[] input;
    private final Pipe<SequentialFileControlSchema>[] control;
    private final Pipe<SequentialFileResponseSchema>[] response;
    private final String[] paths;
    private Path[] path;
    
    private FileChannel[] fileChannel;
    private int[] shutdownInProgress; // 0 not, 1 going, 2 sent
    private int shutdownCount;
    
    private boolean[] releaseRead; //all false for initial state
    private ByteBuffer[] buffA;
    private ByteBuffer[] buffB;
    private byte[] mode; //all WRITE ZERO for initial state

    private FileSystemProvider provider;
    private FileSystem fileSystem;
    private Set<OpenOption> options;

    public SequentialFileReadWriteStage(GraphManager graphManager,
	    						 Pipe<SequentialFileControlSchema>[] control,
	    						 Pipe<SequentialFileResponseSchema>[] response,
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

    @Override
    public void startup() {

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
	    				
	    				PipeWriter.publishEOF(response[i]); //TODO: convert to all low level ....		        	
	    				Pipe.publishEOF(output[i]);
	    				
	    				try {
	            			if (null!=fileChannel[i]) {
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
	    				shutdownInProgress[i] = 2;//only send EOF once
	    			}
	    		} else {
	    		    		
	    		
		    		//each of the possible activities will only require one response
		    		if (PipeWriter.hasRoomForWrite(response[i])) {
		    			
		    			if (
		   					//do not pick up new controls until the writes are complete
		    				(MODE_WRITE == mode[i]) && (hasDataToWrite(i) || Pipe.hasContentToRead(input[i]))
		    				) {
		    				didWork |= writeProcessing(i); //may write to response upon ack of each block single msg with block counts
		    			
		    			} else if (readControl(i)) { //readControl may write to response for meta single msg
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
		Pipe<SequentialFileControlSchema> localControl = control[idx];
		boolean continueWithIdx = true;
		/////////
		//do not use while, commands are rare and we should only do 1
		//////////
		if (PipeReader.tryReadFragment(localControl)) {
		    int msgIdx = PipeReader.getMsgIdx(localControl);
		    switch(msgIdx) {
		        case SequentialFileControlSchema.MSG_REPLAY_1:
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
		        case SequentialFileControlSchema.MSG_CLEAR_2:
		        	//if in write mode set position to zero
		        	
		        	if (MODE_WRITE == mode[idx]) {
		        		//due to the above logic clear is only called after the write pipe is empty.
		        		positionToBeginning(idx);
		        		//no more data should be written until this gets consumed
		        		SequentialFileResponseSchema.publishClearAck(response[idx]);		        		
		        	} else {
		        		logger.info("error, clear called while in read mode");
		        	}
		        	
		        	
				break;
		        case SequentialFileControlSchema.MSG_METAREQUEST_3:
					try {
						BasicFileAttributes readAttributes = this.provider.readAttributes(path[idx], BasicFileAttributes.class);
						
						SequentialFileResponseSchema.publishMetaResponse(
								response[idx], 
								readAttributes.size(), 
								readAttributes.lastModifiedTime().toMillis());
						
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
		        	
					//send mete data but also break out of above since we took the message
		        	continueWithIdx = false;
		        	
				break;
		        case -1:
		        	shutdownInProgress[idx] = 1;
		        break;
		    }
		    PipeReader.releaseReadLock(localControl);
		}
		return continueWithIdx;
	}

	private void positionToBeginning(int idx) {
		try {
			fileChannel[idx].position(0);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
                
                //We write the -1 len as a marker to those down stream that the end has been reached.
                Pipe.addMsgIdx(localOutput, 0);
                Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int)len, localOutput);  
                Pipe.confirmLowLevelWrite(localOutput, SIZE);
                Pipe.publishWrites(localOutput);    

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
	
		int fieldFragmentCount = 0;
		do {
        
        if (null==buffA[idx] && 
            null==buffB[idx]) {
            //read the next block
            
            if (releaseRead[idx]) {
                //only done after we have consumed the bytes
                Pipe.confirmLowLevelRead(localInput, SIZE);
                Pipe.releaseReadLock(localInput);
                releaseRead[idx] = false;
                fieldFragmentCount++;
            }

            if (Pipe.hasContentToRead(localInput)) {
                int msgId      = Pipe.takeMsgIdx(localInput);   
                if (msgId < 0) {
                	ackFinishedWrites(idx, fieldFragmentCount);
                    Pipe.confirmLowLevelRead(localInput, Pipe.EOF_SIZE);
                    Pipe.releaseReadLock(localInput);
                    requestShutdown();
                    return fieldFragmentCount>0;
                }
                assert(0==msgId);
                int meta = Pipe.takeRingByteMetaData(localInput); //for string and byte array
                int len = Pipe.takeRingByteLen(localInput);

                if (len < 0) {
                	ackFinishedWrites(idx, fieldFragmentCount);
                    Pipe.confirmLowLevelRead(localInput, SIZE);
                    Pipe.releaseReadLock(localInput);
                    requestShutdown();
                    return fieldFragmentCount>0;
                }
                
                                                
                releaseRead[idx] = true;
                buffA[idx] = Pipe.wrappedBlobReadingRingA(localInput, meta, len);
                buffB[idx] = Pipe.wrappedBlobReadingRingB(localInput, meta, len);
                if (!buffB[idx].hasRemaining()) {
                    buffB[idx] = null;
                }
                
                
            } else {
            	ackFinishedWrites(idx, fieldFragmentCount);
                return fieldFragmentCount>0;
            }
            
        }

        
        //we have existing data to be written
        if (hasDataToWrite(idx)) {
            try {
                
            	localFileChannel.write(buffA[idx]);
                if (0==buffA[idx].remaining()) {
                    buffA[idx] = null;
                } else {
                	ackFinishedWrites(idx, fieldFragmentCount);
                    return fieldFragmentCount>0;//do not write B because we did not finish A
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
		return fieldFragmentCount>0;
	}

	private void ackFinishedWrites(int idx, int fieldFragmentCount) {
		if (fieldFragmentCount>0) {
        	SequentialFileResponseSchema.publishWriteAck(response[idx], fieldFragmentCount);
        }
	}

	private boolean hasDataToWrite(int idx) {
		return null!=buffA[idx];
	}


}
