package com.ociweb.pronghorn.pipe.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

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
    
    private long readStartTime;
    private long writeStartTime;
    
    private long totalBytesRead;
    private long totalBytesWritten;
    
    
    public StreamRegulator(long bitPerSecond, int maxWrittenChunksInFlight, int maxWrittenChunkSizeInBytes) {
        PipeConfig<RawDataSchema> pipeConfig = new PipeConfig<RawDataSchema>(RawDataSchema.instance, maxWrittenChunksInFlight, maxWrittenChunkSizeInBytes);
        this.pipe = new Pipe<RawDataSchema>(pipeConfig);
        this.pipe.initBuffers();
        Pipe.setPublishBatchSize(pipe, 0); //TODO: why can this not batch publish?
        Pipe.setReleaseBatchSize(pipe, maxWrittenChunksInFlight/3);
        
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
        
    
    /**
     * Blocks until the desired time as passed to ensure this stream conforms to the requested bits per second.
     * Once the time has passed it returns so the InputStream can read the next chunk.
     */
    public boolean hasNextChunk() {
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
        long bitsRead = 8 * totalBytesRead;

        long expectedTime = readStartTime+ (bitsRead*1000/bitPerSecond);
        if (expectedTime>now) {
            try {
                Thread.sleep(expectedTime-now);
            } catch (InterruptedException e) {
                shutdown("Interrupted");
            }
        }
    }

    private boolean readChunk() {
        if (0 == readStartTime) {
            readStartTime = System.currentTimeMillis();
        }
        if (hasOpenRead) {
            //log.trace("release block");
            Pipe.confirmLowLevelRead(pipe, Pipe.sizeOf(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
            Pipe.releaseReads(pipe);
            hasOpenRead = false;
        }
        if (!Pipe.hasContentToRead(pipe)) {
            return false;
        }   
        
        int msgIdx = Pipe.takeMsgIdx(pipe);
        if (RawDataSchema.MSG_CHUNKEDSTREAM_1 == msgIdx) {
            totalBytesRead = totalBytesRead+(inputStreamFlyweight.openLowLevelAPIField());
            hasOpenRead = true;
            return true;
        } else {
            shutdown("EOF Message detected.");
        }
        return false;
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
    
    
    /**
     * Blocks until the desired time as passed to ensure this stream conforms to the requested bits per second.
     * Once the time has passed the output stream buffer will send its data and be ready for more writes.
     */ 
    public boolean hasRoomForChunk() {
        
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
        long bitsWritten = 8 * totalBytesWritten;

        long expectedTime = writeStartTime+ (bitsWritten*1000/bitPerSecond);
        if (expectedTime>now) {
            try {
                Thread.sleep(expectedTime-now);
            } catch (InterruptedException e) {
                shutdown("Interrupted");
            }
        }
    }

    private boolean openForWrite() {
        if (0 == writeStartTime) {
            writeStartTime = System.currentTimeMillis();
        }
        
        if (Pipe.isInBlobFieldWrite(pipe)) {
            //log.trace("write block");
            totalBytesWritten = totalBytesWritten + (outputStreamFlyweight.closeLowLevelField());
            Pipe.confirmLowLevelWrite(pipe, Pipe.sizeOf(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1));
            Pipe.publishWrites(pipe);
        }
        
        if (!Pipe.hasRoomForWrite(pipe)) {
            return false;
        }
        Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
        outputStreamFlyweight.openField();
        return true;
    }

    public long getBytesWritten() {
        return totalBytesWritten;
    }
    
    public long getBytesRead() {
        return totalBytesWritten;
    }
    
    
    
}
