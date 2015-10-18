package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FileBlobWriteStage extends PronghornStage{

    private static final int SIZE = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
    private final RandomAccessFile outputFile;
    private final Pipe<RawDataSchema> input;
    private FileChannel openChannel;
    
    private ByteBuffer buffA;
    private ByteBuffer buffB;
    private boolean releaseRead = false;
    
    public FileBlobWriteStage(GraphManager graphManager, Pipe<RawDataSchema> input, RandomAccessFile outputFile) {
        super(graphManager, input, NONE);
        this.outputFile = outputFile;
        this.input = input;
        
        //can not batch up releases of consumed blocks.  (TODO: Not sure why this is true)
        this.supportsBatchedRelease = false;
    }
    
    //TODO: add second constructor and logic to enable toggle of write between two files

    @Override
    public void startup() {
        openChannel = outputFile.getChannel();
    }
        
    @Override
    public void run() {
                
        if (null==buffA && 
            null==buffB) {
            //read the next block
            
            if (releaseRead) {
                //only done after we have consumed the bytes
                Pipe.confirmLowLevelRead(input, SIZE);
                Pipe.releaseReads(input);
                releaseRead = false;
            }

            if (Pipe.hasContentToRead(input)) {
                int msgId      = Pipe.takeMsgIdx(input);   
                if (msgId < 0) {
                    Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
                    Pipe.releaseReads(input);
                    requestShutdown();
                    return;
                }
                assert(0==msgId);
                int meta = Pipe.takeRingByteMetaData(input); //for string and byte array
                int len = Pipe.takeRingByteLen(input);
                                
                if (len < 0) {
                    Pipe.confirmLowLevelRead(input, SIZE);
                    Pipe.releaseReads(input);
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
        
    }

    @Override
    public void shutdown() {
        if (releaseRead) {
            //only done after we have consumed the bytes
            Pipe.releaseReads(input);
        }
        
        try {
            openChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
                
    }
    
}
