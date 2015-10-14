package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class TapeReadStage extends PronghornStage {

    private final RandomAccessFile inputFile;
    private FileChannel fileChannel;

    //    private HeaderWritableByteChannel   HEADER_WRAPPER = new HeaderWritableByteChannel();    
    
    private IntBuferWritableByteChannel INT_BUFFER_WRAPPER = new IntBuferWritableByteChannel();
    private final Pipe<RawDataSchema> target;    
    
    private int blobToRead=0;
    private int slabToRead=0;
    private long targetSlabPos;
    private int targetBlobPos;
        
    protected TapeReadStage(GraphManager graphManager, RandomAccessFile inputFile, Pipe<RawDataSchema> output) {
        super(graphManager, NONE, output);
        this.inputFile = inputFile;
        this.target = output;
        
        //TODO: add command pipe for reading from multiple channels
        
    }

    @Override
    public void startup() {
        fileChannel = inputFile.getChannel();
    }
    
    @Override
    public void run() {
        while (processAvailData(this)) {
            //keeps going while there is data to read and room to write it.
        }
    }

    ByteBuffer header = ByteBuffer.allocate(8);
    IntBuffer intHeader = header.asIntBuffer();
    
    private boolean processAvailData(TapeReadStage tapeReadStage) {
        try {
            //read blob count int  (in bytes)
            //read slab count int  (in bytes)
            //read slab ints
            //read blob bytes
            
            if (0==slabToRead && 0==blobToRead) {

                int len = fileChannel.read(header);
                if (len<0) {
                    fileChannel.close();
                    requestShutdown();
                }                
                if (header.hasRemaining()) {
                    //try again we did not get all 8 bytes.
                    return false;
                }
                
                header.flip();
                
                blobToRead = intHeader.get();
                slabToRead = intHeader.get();                
                targetSlabPos = Pipe.workingHeadPosition(target);            
                targetBlobPos = Pipe.bytesWorkingHeadPosition(target);
                
                if (slabToRead>>2 > target.sizeOfSlabRing) {
                    throw new UnsupportedOperationException("Unable to read file into short target pipe.");
                }                
            }
            
            //try later if there is not enough room for this entire block
            if (!Pipe.hasRoomForWrite(target, slabToRead>>2)) {
                return false;
            }
            
            if (slabToRead>0) {
                
                int spaceLeftOnRing = target.sizeOfSlabRing - Pipe.contentRemaining(target);
                int spaceLeftBeforeRollover =   (int)(target.sizeOfSlabRing - (targetSlabPos&target.mask));
                int maxLength = Math.min(Math.min(slabToRead, spaceLeftOnRing), spaceLeftBeforeRollover);
                
                IntBuffer slabBuffer = Pipe.wrappedSlabRing(target);
                slabBuffer.position( (int)targetSlabPos&target.mask );
                slabBuffer.limit(slabBuffer.position() + maxLength);
                
                INT_BUFFER_WRAPPER.init(slabBuffer);
                long filePos = fileChannel.position();  
                long size = fileChannel.transferTo(filePos, slabToRead, INT_BUFFER_WRAPPER);
                targetSlabPos += size;
                if ((slabToRead -= size)>0) {
                    return false;
                }
            }
            if (0==slabToRead && blobToRead>0) {
                                                         
                ByteBuffer byteBuff = Pipe.wrappedBlobForWriting(targetBlobPos, target);                
                byteBuff.limit(Math.min(byteBuff.limit(), byteBuff.position()+blobToRead));
                
                int count = fileChannel.write(byteBuff);
                targetBlobPos += count;
                if ((blobToRead -= count)>0) {
                    return false;
                }
                 
            }
            
            if (0==slabToRead && 0==blobToRead) {
                Pipe.setBytesWorkingHead(target, targetBlobPos);
                Pipe.setBytesHead(target, targetBlobPos);
                Pipe.publishWorkingHeadPosition(target, targetSlabPos);
            }
             
            
        } catch (IOException e) {
            e.printStackTrace();
            
            return false;
        }
        
        return true;
        
    }

    private class IntBuferWritableByteChannel implements WritableByteChannel {

        private IntBuffer buffer;
        
        public void init(IntBuffer intBuffer) {
            buffer = intBuffer;
        }
        
        @Override
        public boolean isOpen() {
            return null!=buffer;
        }

        @Override
        public void close() throws IOException {
            buffer = null;
        }
        
        @Override
        public int write(ByteBuffer src) throws IOException {  
            
            int count = (int)(src.remaining())>>2;
            while (--count>=0) {
                int value = src.get()<<24;
                value |= 0xFF&src.get()<<16;
                value |= 0xFF&src.get()<<8;
                value |= 0xFF&src.get();
                
                buffer.put(value);
                
            }
            return count<<2;
        }
        
    }

}
