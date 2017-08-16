package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FileBlobWriteStage extends PronghornStage{

    private static final int SIZE = RawDataSchema.FROM.fragDataSize[0];

    private final Pipe<RawDataSchema> input;
    private FileChannel[] fileChannel;
    
    private ByteBuffer buffA;
    private ByteBuffer buffB;
    private boolean releaseRead = false;
    
    private FileSystemProvider provider;
    private FileSystem fileSystem;
    private Set<OpenOption> writeOptions;
    private String[] outputPathString;

    private int selectedFile = 0;
    
    private final boolean append;
    
    public FileBlobWriteStage(GraphManager graphManager,
    		                  Pipe<RawDataSchema> input,
    		                  //add pipe to select file.
    		                  boolean append, 
    		                  String ... outputPathString) {
    	
        super(graphManager, input, NONE);

        this.append = append;
        this.outputPathString = outputPathString;
        
        this.input = input;
        
        //TODO: Needs to rotate to the next file upon reaching a specific size.
        //   upon switch we need to notify someone??
        
        
        
    }

    @Override
    public void startup() {

        this.fileSystem = FileSystems.getDefault();
        this.provider = fileSystem.provider();
        this.writeOptions = new HashSet<OpenOption>();

        this.writeOptions.add(StandardOpenOption.SYNC);
        this.writeOptions.add(StandardOpenOption.CREATE);

        if (append) {
        	this.writeOptions.add(StandardOpenOption.APPEND);        	
        } else {
        	this.writeOptions.add(StandardOpenOption.TRUNCATE_EXISTING);
        }
        
        this.writeOptions.add(StandardOpenOption.WRITE);
        

        try {
        	int i = outputPathString.length;
        	fileChannel = new FileChannel[i];
        	while (--i>=0) {
        		fileChannel[i] = provider.newFileChannel(fileSystem.getPath(outputPathString[i]), writeOptions);     		
     
        	}
        	
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        
    }

    //This will probably never work since it requires strict state control
    private void selectFile(int idx) {
    	selectedFile = idx;
    }
        
    private void clearFile() {
    	
    	try {
			fileChannel[selectedFile].position(0);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    	
    }
    
    
    @Override
    public void run() {
             
        writeProcessing();
        
    }

	private void writeProcessing() {
		do {
        
        if (null==buffA && 
            null==buffB) {
            //read the next block
            
            if (releaseRead) {
                //only done after we have consumed the bytes
                Pipe.confirmLowLevelRead(input, SIZE);
                Pipe.releaseReadLock(input);
                releaseRead = false;
            }

            if (Pipe.hasContentToRead(input)) {
                int msgId      = Pipe.takeMsgIdx(input);   
                if (msgId < 0) {
                    Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
                    Pipe.releaseReadLock(input);
                    requestShutdown();
                    return;
                }
                assert(0==msgId);
                int meta = Pipe.takeRingByteMetaData(input); //for string and byte array
                int len = Pipe.takeRingByteLen(input);
                                
                if (len < 0) {
                    Pipe.confirmLowLevelRead(input, SIZE);
                    Pipe.releaseReadLock(input);
                    requestShutdown();
                    return;
                }
                
                                                
                releaseRead = true;
                buffA = Pipe.wrappedBlobReadingRingA(input, meta, len);
                buffB = Pipe.wrappedBlobReadingRingB(input, meta, len);
                if (!buffB.hasRemaining()) {
                    buffB = null;
                }
                
                
            } else {
                //there is nothing to read
                return;
            }
            
        }
        
        //we have existing data to be written
        if (null!=buffA) {
            try {
                
                 fileChannel[selectedFile].write(buffA);
                if (0==buffA.remaining()) {
                    buffA = null;
                } else {
                    return;//do not write B because we did not finish A
                }
                
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        if (null!=buffB) {
            try {                
                fileChannel[selectedFile].write(buffB);
                if (0==buffB.remaining()) {
                    buffB = null;
                }
                
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        } while (null == buffA && null == buffB);
	}

    @Override
    public void shutdown() {
        if (releaseRead) {
            //only done after we have consumed the bytes
            Pipe.releaseReadLock(input);
        }
        
    	int i = fileChannel.length;
    	while (--i>=0) {
    		try {
    			fileChannel[i].close();
    		} catch (IOException e) {
    			throw new RuntimeException(e);
    		}
    	}
                
    }
    
}
