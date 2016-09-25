package com.ociweb.pronghorn.pipe.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class StreamRegulator {
    
    private final Logger log = LoggerFactory.getLogger(StreamRegulator.class);

    private final Pipe<RawDataSchema> pipe;    
    private final DataInputBlobReader<RawDataSchema> inputStreamFlyweight;
    private final DataOutputBlobWriter<RawDataSchema> outputStreamFlyweight;
    private final long bitPerSecond;

    private boolean hasOpenRead;
    private boolean hasOpenWrite;
    
    private long readStartTime;
    private long writeStartTime;
    
    private long totalBytesRead;
    private long totalBytesWritten;
    
    private static final int MSG_SIZE = RawDataSchema.FROM.fragDataSize[RawDataSchema.MSG_CHUNKEDSTREAM_1];
    
    public StreamRegulator(long bitPerSecond, int maxWrittenChunksInFlight, int maxWrittenChunkSizeInBytes) {
        PipeConfig<RawDataSchema> pipeConfig = new PipeConfig<RawDataSchema>(RawDataSchema.instance, maxWrittenChunksInFlight, maxWrittenChunkSizeInBytes);
        this.pipe = new Pipe<RawDataSchema>(pipeConfig);
        this.pipe.initBuffers();
        Pipe.setPublishBatchSize(pipe, 0); 
        Pipe.setReleaseBatchSize(pipe, maxWrittenChunksInFlight/3);
        
        if (this.pipe.byteMask<=0) {
            throw new UnsupportedOperationException("Pipe must have room to send blob data. Found size:"+ this.pipe.sizeOfBlobRing+" config: "+pipeConfig);
        }
        
        this.inputStreamFlyweight = new DataInputBlobReader<RawDataSchema>(pipe);
        this.outputStreamFlyweight = new DataOutputBlobWriter<RawDataSchema>(pipe);
        this.bitPerSecond = bitPerSecond;
        //TODO: may want to add latency per chunk, per startup, or per N bytes.
    }
    
    public String toString() {
        return pipe.toString();
        
    }
    
    public InputStream getInputStream() {   
        return inputStreamFlyweight;
    }
    
    public DataInput getDataInput() {        
        return inputStreamFlyweight;
    }
    
    public DataInputBlobReader<RawDataSchema> getBlobReader() {
        return inputStreamFlyweight;
    }

        
    
    /**
     * Blocks until the desired time as passed to ensure this stream conforms to the requested bits per second.
     * Once the time has passed it returns so the InputStream can read the next chunk.
     */
    public final boolean hasNextChunk() {
        if (!readChunk()) {
            return false; //No chunk now try again
        }
        waitAsNeededForRead();
        return true;
    }

    private void waitAsNeededForRead() {
        long now = System.currentTimeMillis();
        if (readStartTime==now) {
            return;
        }
        long bitsRead = 8L * totalBytesRead;

        long expectedTime = readStartTime+ (bitsRead*1000L/bitPerSecond);
        if (expectedTime>now) {
            try {
                Thread.sleep(expectedTime-now);
            } catch (InterruptedException e) {
                shutdown("Interrupted");
            }
        }
    }

    private boolean readChunk() {
        readPrep();
        if (Pipe.hasContentToRead(pipe)) {
            return beginNewRead();
        }   
        return false;
    }

    private boolean beginNewRead() {
        int msgIdx = Pipe.takeMsgIdx(pipe);
        if (RawDataSchema.MSG_CHUNKEDSTREAM_1 == msgIdx) {
            totalBytesRead = totalBytesRead+inputStreamFlyweight.openLowLevelAPIField();
            hasOpenRead = true;
            return true;
        } else {
            shutdown("EOF Message detected.");
        }
        return false;
    }

    private void readPrep() {
        if (hasOpenRead) {
            releaseOpenRead();
        } else if (0 == readStartTime) {
                readStartTime = System.currentTimeMillis();
        }
    }

    
    private void releaseOpenRead() {
        //log.trace("release block");
        Pipe.confirmLowLevelRead(pipe, MSG_SIZE);
        Pipe.releaseReadLock(pipe);
        hasOpenRead = false;
    }
        
    public void shutdown() {
        shutdown(null);
    }
    
    private void shutdown(String reason) {
        if (null!=reason) {
            log.warn(reason);
        }
        
        try {
            inputStreamFlyweight.close();
            outputStreamFlyweight.close();
        } catch (IOException e) { 
            throw new RuntimeException(e);
        }
    }
    
    public OutputStream getOutputStream() {    
        return outputStreamFlyweight;
    }
    
    public DataOutput getDataOutput() {
        return outputStreamFlyweight;
    }
    
    //TODO: add support for WHY there is no room, return sleep duration?
    
    
    /**
     * Blocks until the desired time as passed to ensure this stream conforms to the requested bits per second.
     * Once the time has passed the output stream buffer will send its data and be ready for more writes.
     */ 
    public final boolean hasRoomForChunk() {        
        if (!openForWrite()) {
            return false;
        };
        waitAsNeededForWrite();
        return true;
    }

    private void waitAsNeededForWrite() {
        long now = System.currentTimeMillis();
        if (writeStartTime==now) {
            return;
        }
        long bitsWritten = 8L * totalBytesWritten;

        long expectedTime = writeStartTime+ (bitsWritten*1000L/bitPerSecond);
        if (expectedTime>now) {
            try {
                Thread.sleep(expectedTime-now);
            } catch (InterruptedException e) {
                shutdown("Interrupted");
            }
        }
    }

    private boolean openForWrite() {
        writePrep();        
        if (Pipe.hasRoomForWrite(pipe)) {
            return beginNewWrite();
        }
        return false;
    }

    private boolean beginNewWrite() {
        Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
        outputStreamFlyweight.openField();
        hasOpenWrite = true;
        return true;
    }

    private void writePrep() {        
        if (hasOpenWrite) {
            publishOpenWrite();
        } else if (0 == writeStartTime) {
            writeStartTime = System.currentTimeMillis();
        }
    }

    private void publishOpenWrite() {
        //log.trace("write block");
        totalBytesWritten = totalBytesWritten + (outputStreamFlyweight.closeLowLevelField());        
        Pipe.confirmLowLevelWrite(pipe, Pipe.sizeOf(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
        Pipe.publishWrites(pipe);
        hasOpenWrite = false;
    }

    public long getBytesWritten() {
        return totalBytesWritten;
    }
    
    public long getBytesRead() {
        return totalBytesWritten;
    }
        
    public DataOutputBlobWriter<RawDataSchema> getBlobWriter() {
        return outputStreamFlyweight;
    }
    
}
