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
    private final RandomAccessFile outputFile;
    private final Pipe<RawDataSchema> input;
    private ByteChannel openChannel;
    
    private ByteBuffer buffA;
    private ByteBuffer buffB;
    private boolean releaseRead = false;
    
    private FileSystemProvider provider;
    private FileSystem fileSystem;
    private Set<OpenOption> writeOptions;
    private String outputPathString;
    
    public FileBlobWriteStage(GraphManager graphManager, Pipe<RawDataSchema> input, RandomAccessFile outputFile, String outputPathString) {
        super(graphManager, input, NONE);
        this.outputFile = outputFile;
        this.outputPathString = outputPathString;
        this.input = input;
        
        //can not batch up releases of consumed blocks.  (TODO: Not sure why this is true)
        this.supportsBatchedRelease = false;
    }
    
    //TODO: add second constructor and logic to enable toggle of write between two files

    @Override
    public void startup() {
        openChannel = outputFile.getChannel();

//        //TODO: investigate why the above getChannel() is so much faster than the below .
//        
//        this.fileSystem = FileSystems.getDefault();
//        this.provider = fileSystem.provider();
//        this.writeOptions = new HashSet<OpenOption>();
//
//        this.writeOptions.add(StandardOpenOption.CREATE);
//        this.writeOptions.add(StandardOpenOption.TRUNCATE_EXISTING);
//        this.writeOptions.add(StandardOpenOption.WRITE);
//        
//        Path path = fileSystem.getPath(outputPathString);
//       
//        try {
//            //openChannel = provider.newByteChannel(path, writeOptions);
//          //  ExecutorService executor = Executors.newFixedThreadPool(2);
//            //openChannel = provider.newAsynchronousFileChannel(path, writeOptions, executor);
//            openChannel = provider.newFileChannel(path, writeOptions);
////            openChannel.position(0);
////            openChannel.force(false);
//            
//        } catch (IOException e) {
//           throw new RuntimeException(e);
//        } 
//        
        
    }
        
    @Override
    public void run() {
             
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
                
                 openChannel.write(buffA);
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
                openChannel.write(buffB);
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
        
        try {
            openChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
                
    }
    
}
