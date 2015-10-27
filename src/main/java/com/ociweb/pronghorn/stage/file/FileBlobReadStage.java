package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FileBlobReadStage extends PronghornStage {

    private static final int SIZE = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
    private final RandomAccessFile inputFile;
    private FileChannel openChannel;
    private final Pipe<RawDataSchema> output;
    
    public FileBlobReadStage(GraphManager graphManager, RandomAccessFile inputFile, Pipe<RawDataSchema> output) {
        super(graphManager, NONE, output);
        this.inputFile = inputFile;
        this.output = output;
        
    }

    @Override
    public void startup() {
        openChannel = inputFile.getChannel();
    }

    @Override
    public void run() {
            
        while (Pipe.hasRoomForWrite(output)) {
        
            int originalBlobPosition = Pipe.bytesWorkingHeadPosition(output);      
            try {            
                
                Pipe.addMsgIdx(output, 0);
                //attempt to read this many bytes but may read less
                int len = openChannel.read(Pipe.wrappedBlobForWriting(originalBlobPosition, output));
                if (len<0) {
                    Pipe.publishAllBatchedWrites(output);
                    requestShutdown();
                    return;
                }       
                Pipe.moveBlobPointerAndRecordPosAndLength(originalBlobPosition, len, output);  
                Pipe.confirmLowLevelWrite(output, SIZE);
                Pipe.publishWrites(output);    
                
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
