package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashSet;
import java.util.Set;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Reads files from disk as blobs based on an input path.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class FileBlobReadStage extends PronghornStage {

    private static final int SIZE = RawDataSchema.FROM.fragDataSize[0];
    
    private final String inputPathString;
    private FileChannel fileChannel;
    
    private final Pipe<RawDataSchema> output;
    
    private FileSystemProvider provider;
    private FileSystem fileSystem;
    private Set<OpenOption> readOptions;
    private boolean shutdownInProgress;

    /**
     *
     * @param graphManager
     * @param output _out_ Pipe that file from inputPathString will be written to.
     * @param inputPathString
     */
    public FileBlobReadStage(GraphManager graphManager, 
    						 //add input pipe to select file to read
    		                 Pipe<RawDataSchema> output, 
    		                 String inputPathString) {

    	//TODO: add second constructor which takes input control pipe to reset position.
    	
        super(graphManager, NONE, output);
        this.inputPathString = inputPathString;
        this.output = output;

        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "cornsilk2", this);
        
        if (null==inputPathString || inputPathString.length()==0) {
        	//do not bother running if we have no file.
        	GraphManager.addNota(graphManager, GraphManager.UNSCHEDULED, GraphManager.UNSCHEDULED, this);
        }
        
    }

    public static FileBlobReadStage newInstance(GraphManager graphManager,
                                                //add input pipe to select file to read
                                                Pipe<RawDataSchema> output,
                                                String inputPathString) {
    	
        return new FileBlobReadStage(graphManager, output, inputPathString);
    }

    @Override
    public void startup() {
    	if (null!=inputPathString && inputPathString.length()>0) {
	        this.fileSystem = FileSystems.getDefault();
	        this.provider = fileSystem.provider();
	        this.readOptions = new HashSet<OpenOption>();
	        this.readOptions.add(StandardOpenOption.READ);
	        this.readOptions.add(StandardOpenOption.SYNC);
	        
	        try {
	        	fileChannel = provider.newFileChannel(fileSystem.getPath(inputPathString), readOptions);
	        } catch (IOException e) {
	           throw new RuntimeException(e);
	        } 
    	}
    }


    private void repositionToBeginning() {
    	try {
			fileChannel.position(0);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }
    
    @Override
    public void run() {
    	if (fileChannel!=null) {
		   	 if(shutdownInProgress) {
	         	if (null!=output && Pipe.isInit(output)) {
	         		if (!Pipe.hasRoomForWrite(output, Pipe.EOF_SIZE)){ 
	         			return;
	         		}  
	         	}
		        requestShutdown();
		        return;
			 }
	   	 
	        while (Pipe.hasRoomForWrite(output)) {
	            //System.err.println("has room for write");
	            int originalBlobPosition = Pipe.getWorkingBlobHeadPosition(output);      
	            try {            
	                
	                //attempt to read this many bytes but may read less
	                long len = fileChannel.read(Pipe.wrappedWritingBuffers(originalBlobPosition, output));
	                if (len>0) {
	                    Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	                    Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, (int)len, output);  
	                    Pipe.confirmLowLevelWrite(output, SIZE);
	                    Pipe.publishWrites(output);    
	                } else if (len<0) {
	                	//signal to upstream stages that we are done with the data
	                	Pipe.addMsgIdx(output, RawDataSchema.MSG_CHUNKEDSTREAM_1);
	                	Pipe.addNullByteArray(output);
	                	Pipe.confirmLowLevelWrite(output, SIZE);
	                    Pipe.publishWrites(output);
	                	                    
	                    Pipe.publishAllBatchedWrites(output);
	                    shutdownInProgress = true;
	                    return;
	                } 
	            } catch (IOException e) {
	               throw new RuntimeException(e);
	            }
	        }       
    	} else {
    		requestShutdown();
    	}
    }

    @Override
    public void shutdown() {
	    	if (null!=output && Pipe.isInit(output)) {
	    	    Pipe.publishEOF(output);   
	    	}
        	
	    	try {
	    		fileChannel.close();
	    	} catch (IOException e) {
	    		throw new RuntimeException(e);
	    	}
    }

}
