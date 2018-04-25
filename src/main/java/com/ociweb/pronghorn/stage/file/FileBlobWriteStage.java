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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.ISOTimeFormatterLowGC;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

//TODO: should roll up writes when possible.
//TODO: update to use byteBuffer array...

public class FileBlobWriteStage extends PronghornStage{

    private static final long FILE_ROTATE_SIZE = 1L<<27;
	private static final int SIZE = RawDataSchema.FROM.fragDataSize[0];
    private static final Logger logger = LoggerFactory.getLogger(FileBlobWriteStage.class);
    
    private final Pipe<RawDataSchema> input;
    private FileChannel fileChannel;
    
    private ByteBuffer buffA;
    private ByteBuffer buffB;
    private boolean releaseRead = false;
    
    private FileSystemProvider provider;
    private FileSystem fileSystem;
    private Set<OpenOption> writeOptions;
    
    ////////////////////////
    //file patterns  <BasePath>YYYYMMDDHHMMsssss.log
    //old files are deleted while running
    //upon restart the old files are NOT deleted.
    //keep ring of old files to be deleted.
    ////////////////////////
    
    private final int maxFileCount;
    private String[] absoluteFileNames;//so the old one can be deleted
    private final String basePath;

    private int selectedFile = 0;
    private final long fileRotateSize;
    
    private final boolean append;
    private StringBuilder pathBuilder;
    private ISOTimeFormatterLowGC formatter;
    
    public FileBlobWriteStage(GraphManager graphManager,
            Pipe<RawDataSchema> input,
            //add pipe to select file.
            boolean append, 
            String outputPathString) {
    	this(graphManager, input, FILE_ROTATE_SIZE, append, outputPathString, 1);
    }
    
    //fileRotateSize is ignored if there is only 1 file in the output path strings
    public FileBlobWriteStage(GraphManager graphManager,
    		                  Pipe<RawDataSchema> input,
    		                  long fileRotateSize,
    		                  boolean append, 
    		                  String pathBase,
    		                  int maxFileCount) {
    	
        super(graphManager, input, NONE);
        assert(pathBase!=null);
        this.append = append;
        this.fileRotateSize = fileRotateSize;
        this.input = input;
        this.maxFileCount = maxFileCount;
        this.basePath = pathBase;
    	
    	//TODO: need to have external config of file size and count
    	//TODO: need file names to rotate forward with timestamps when created.
    	//TODO: refine business latency..
        
        
        GraphManager.addNota(graphManager, GraphManager.ISOLATE, GraphManager.ISOLATE, this);
        GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
        long LARGE_SLA_FOR_FILE_WRITE = 10_000_000_000L;
        GraphManager.addNota(graphManager, GraphManager.SLA_LATENCY, LARGE_SLA_FOR_FILE_WRITE, this);
        
        
    }

    @Override
    public void startup() {

    	this.formatter = new ISOTimeFormatterLowGC(true);
    			
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
        
        this.absoluteFileNames = new String[maxFileCount];
        this.pathBuilder = new StringBuilder();
        
        try {
        	assert(0==selectedFile);
        	this.absoluteFileNames[selectedFile] =  generateFileName();
        	        	
        	fileChannel = provider.newFileChannel(
        			fileSystem.getPath(this.absoluteFileNames[selectedFile]), writeOptions);
        
        } catch (IOException e) {
           throw new RuntimeException(e);
        }
        
    }    
    
    
    private String generateFileName() {
    	
    	if (maxFileCount==1) {
    		return basePath;//no rotation or timestamps
    	} else {
    		pathBuilder.setLength(0);
    		pathBuilder.append(basePath);
    		formatter.write(System.currentTimeMillis(), pathBuilder);
    		pathBuilder.append(".log");
    		return pathBuilder.toString();
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

            //only rotate when we have more than 1 file 
            if (maxFileCount>1) {
	            try {
					long fileSize = fileChannel.size();
					if (fileSize>fileRotateSize) {
						//close file
						fileChannel.close();
						
						//rotate to next file.
						if (++selectedFile == maxFileCount) {
							selectedFile = 0;
						}
						
						String oldName = "";
						if (null!=absoluteFileNames[selectedFile]) {
							//before we replace this file we must delete if it is found							
							provider.delete(fileSystem.getPath(oldName = absoluteFileNames[selectedFile]));							
						}
						
						absoluteFileNames[selectedFile] = generateFileName();
						if (absoluteFileNames[selectedFile].equals(oldName)) {
							logger.warn("log file names are not unique because the file sizes are too small, increase max size.");
							//to allow for continued use modify the name to avoid collision.
							absoluteFileNames[selectedFile] += (""+Math.random());
						}
						
						fileChannel = provider.newFileChannel(
			        			 fileSystem.getPath(absoluteFileNames[selectedFile]), writeOptions);
					}
					
					
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
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
                
                 fileChannel.write(buffA);
                if (0==buffA.remaining()) {
                    buffA = null;
                } else {
                    return;//do not write B because we did not finish A
                }
                
                if (this.didWorkMonitor != null) {
        			didWorkMonitor.published(); //did work but we are batching.
        		}
                
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
        if (null!=buffB) {
            try {                
                fileChannel.write(buffB);
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
        if (fileChannel.isOpen()) {
        	try {
				fileChannel.close();
			} catch (IOException e) {
				logger.info("unable to close ",e);
			}
        }
                
    }
    
}
