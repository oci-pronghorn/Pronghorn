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
    
    protected TapeReadStage(GraphManager graphManager, FileChannel fileChannel, Pipe<T> target) {
        super(graphManager, NONE, target);
        this.fileChannel = fileChannel;
        this.target = target;
    }

    @Override
    public void run() {
        while (processAvailData(this)) {
            //keeps going while there is data to read and room to write it.
        }

    }

    private boolean processAvailData(TapeReadStage<T> tapeReadStage) {
        try {
            
            fileChannel.transferTo(0, HEADER_WRAPPER.init(), HEADER_WRAPPER);                     
            
            int blobBytes = HEADER_WRAPPER.blobBytes;
            int slabBytes = HEADER_WRAPPER.slabBytes;            
            
            fileChannel.transferTo(0,  INT_BUFFER_WRAPPER.init(Pipe.wrappedStructuredLayoutRingBuffer(tapeReadStage.target)), INT_BUFFER_WRAPPER);

            //TODO: point to right block for write.
            int off =0;
            int len = 0;
            fileChannel.read(Pipe.wrappedUnstructuredLayoutRingBufferA(tapeReadStage.target));
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        
        // TODO Auto-generated method stub
        return false;
        
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
            int result = src.remaining();
            buffer.put(src.asIntBuffer());
            return result;
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
