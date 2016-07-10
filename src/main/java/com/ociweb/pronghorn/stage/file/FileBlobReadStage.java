package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.HashSet;
import java.util.Set;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FileBlobReadStage extends PronghornStage {

    private static final int SIZE = RawDataSchema.FROM.fragDataSize[0];
    
    private final String inputPathString;
    
    private FileChannel openChannel;
    private final Pipe<RawDataSchema> output;
    
    private FileSystemProvider provider;
    private FileSystem fileSystem;
    private Set<OpenOption> readOptions;
    
    public FileBlobReadStage(GraphManager graphManager, String inputPathString, Pipe<RawDataSchema> output) {
        super(graphManager, NONE, output);
        this.inputPathString = inputPathString;
        this.output = output;
        
    }

    @Override
    public void startup() {
        this.fileSystem = FileSystems.getDefault();
        this.provider = fileSystem.provider();
        this.readOptions = new HashSet<OpenOption>();
        this.readOptions.add(StandardOpenOption.READ);
        this.readOptions.add(StandardOpenOption.SYNC);
        
        Path path = fileSystem.getPath(inputPathString);
        try {
            openChannel = provider.newFileChannel(path, readOptions);
        } catch (IOException e) {
           throw new RuntimeException(e);
        } 
        
        
    }

    @Override
    public void run() {
            
        while (Pipe.hasRoomForWrite(output)) {
        
            int originalBlobPosition = Pipe.getBlobWorkingHeadPosition(output);      
            try {            
                
                //attempt to read this many bytes but may read less
                int len = openChannel.read(Pipe.wrappedBlobForWriting(originalBlobPosition, output));
                if (len>0) {
                    Pipe.addMsgIdx(output, 0);
                    Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, len, output);  
                    Pipe.confirmLowLevelWrite(output, SIZE);
                    Pipe.publishWrites(output);    
                } else if (len<0) {
                    Pipe.publishAllBatchedWrites(output);
                    requestShutdown();
                    return;
                }     
                
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
        }
                
    }

    @Override
    public void shutdown() {
        try {
            openChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
