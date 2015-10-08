package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class TapeReadStage<T extends MessageSchema> extends PronghornStage {

    private final FileChannel fileChannel;
    private HeaderWritableByteChannel   HEADER_WRAPPER = new HeaderWritableByteChannel();    
    private IntBuferWritableByteChannel INT_BUFFER_WRAPPER = new IntBuferWritableByteChannel();
    private final Pipe<T> target;    
    
    private int blobToRead=0;
    private int slabToRead=0;
    private ByteBuffer blobByteBuffer;
    
    protected TapeReadStage(GraphManager graphManager, Pipe<T> target, FileChannel fileChannel) {
        super(graphManager, NONE, target);
        this.fileChannel = fileChannel;
        this.target = target;
        
        //TODO: add command pipe for reading from multiple channels
        
    }

    @Override
    public void run() {
        while (processAvailData(this)) {
            //keeps going while there is data to read and room to write it.
        }

    }

    private boolean processAvailData(TapeReadStage<T> tapeReadStage) {
        try {
            
            if (0==slabToRead && 0==blobToRead) {
                if (0==fileChannel.transferTo(0, HEADER_WRAPPER.init(), HEADER_WRAPPER)) {
                    //no data ready to read
                    return false;
                    
                } else {
                    //NOTE: there is no validation on length because the blob data here is multiple messages all run together.
                    //      the general ratio however would still be respected.
                    blobToRead = HEADER_WRAPPER.blobBytes;
                    slabToRead = HEADER_WRAPPER.slabBytes;

                    
                    //
                    
                    //what about looping?
                    
                    //TODO: do these both need to be set to the right positoin?
                    IntBuffer wrappedStructuredLayoutRingBuffer = Pipe.wrappedSlabRing(tapeReadStage.target);
                    blobByteBuffer = Pipe.wrappedBlobRingA(tapeReadStage.target);
                    //TODO: this buffer limit MUST be set.
                    
                    
                    
                    
                    INT_BUFFER_WRAPPER.init(wrappedStructuredLayoutRingBuffer);                    
                }
            }
            
            if (!Pipe.hasRoomForWrite(tapeReadStage.target, slabToRead)) {
                return false;                
            }
            
            
            long slabsRead = 0;
            if (slabToRead>0) {
                slabsRead = fileChannel.transferTo(0, slabToRead, INT_BUFFER_WRAPPER);
            }
            
            if (slabsRead < 0) {
                //error unexpected end of stream
                blobToRead = 0;
                slabToRead = 0;
                return false;
            }            
            slabToRead -= slabsRead;
            
            if (slabToRead > 0) {
                //did not get all the bytes try again later
                return false;
            }
            //all the slab data has been read now we must pick up the blob data
            
            if (blobToRead>0) {
                
                int blobsRead = fileChannel.read(blobByteBuffer);
                
                if (blobsRead<0) {
                    //end of file, we may have been expecting this one if blobToRead and slabToRead are already zero.
                    blobToRead = 0;
                    slabToRead = 0;
                    return false;
                } else {
                    blobToRead -= blobsRead;                    
                    if (blobToRead>0) {
                        //did not get all the bytes try again later
                        return false;
                    }
                }           
                
            }
            
        } catch (IOException e) {
            e.printStackTrace();
            
            return false;
        }
        
        return true;
        
    }

    private class IntBuferWritableByteChannel implements WritableByteChannel {

        private IntBuffer buffer;
        
        public int init(IntBuffer intBuffer) {
            buffer = intBuffer;
            return intBuffer.remaining()*4;
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
            int result = Math.min(src.remaining()>>2, buffer.remaining());
            
            int i = result;
            while (--i>=0) {
                int value = buffer.get();
                src.put((byte)(value>>24));
                src.put((byte)(value>>16));
                src.put((byte)(value>>8));
                src.put((byte)(value>>0));
            }
            return result<<2;
        }
        
    }
    
    private class HeaderWritableByteChannel implements WritableByteChannel {

        private int blobBytes;
        private int slabBytes;
        private boolean isOpen;
        
        public int init() {
            blobBytes = -1;
            slabBytes = -1;
            isOpen = true;
            return 2*4;
        }
        
        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public void close() throws IOException {
            isOpen = false;
        }
        
        @Override
        public int write(ByteBuffer src) throws IOException {
            if (!isOpen()) {
                return -1;
            }
            
            //only read if the full head is available
            if (src.remaining()<(2*4)) {
                return 0;
            }
                       
            IntBuffer intBuffer = src.asIntBuffer();
            blobBytes = intBuffer.get();
            slabBytes = intBuffer.get();
                        
            close();
            return 2*4;            
        }
        
    }
}
