package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.ReadableByteChannel;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Write data from a pipe directly to disk. This records both the slab and blob data without any schema concerns.
 * This is the simplest way to save data in a pipe to disk.
 * 
 * @author Nathan Tippy
 *
 */
public class TapeWriteStage<T extends MessageSchema> extends PronghornStage {

	private FileChannel fileChannel;

	private Pipe<T> source;
	
	private int byteHeadPos;
	private long headPos;
	private long cachedTail;
	private long totalPrimaryCopy;
	
	private int tempByteTail; 
	private int byteTailPos;
	private int totalBytesCopy;
	
	//Header between each chunk must define 
	//           (4)    total bytes of the following block  (both primary and secondary together, to enable skipping)
	//           (4)    byte count of primary (this is always checked first for space when reading)
	
	private ByteBuffer header;
	private IntBuffer  headerInt;
	
	private ByteBuffer blobBuffer1;
	private ByteBuffer blobBuffer2;
    
	private IntBuffer slabBuffer1;
	private IntBuffer slabBuffer2;
    
    private IntBufferAdapter IBA;
    
	
	private final RandomAccessFile outputFile;
	
	///TODO: add second pipe with commands eg (write N fields to X file then close)
	
	public TapeWriteStage(GraphManager gm, Pipe<T> source, RandomAccessFile outputFile) {
		super(gm,source,NONE);
		
        this.outputFile = outputFile;        
        this.source = source;        
        this.cachedTail = Pipe.tailPosition(source);
    
        this.supportsBatchedPublish = false;
        this.supportsBatchedRelease = false;        
        
    }
	
	@Override
	public void startup() {
	    IBA = new IntBufferAdapter();
	    
	    //for paranoid safety we keep our own byteBufferWrappers
	    blobBuffer1 = Pipe.wrappedBlobRingA(source).duplicate();
	    slabBuffer1 = Pipe.wrappedSlabRing(source).duplicate();
	    blobBuffer1.limit(0);
	    slabBuffer1.limit(0);
	    
	    blobBuffer2 = Pipe.wrappedBlobRingB(source).duplicate();
	    slabBuffer2 = Pipe.wrappedSlabRing(source).duplicate();
	    blobBuffer2.limit(0);
	    slabBuffer2.limit(0);
	    
	    header = ByteBuffer.allocate(8);
	    headerInt = header.asIntBuffer();
	    
	    fileChannel = outputFile.getChannel();
	}
    
    @Override
    public void run() {     
        processAvailData(this);//spin here makes little difference, its the ammount of work done which is the problem.
    }

    @Override
    public void shutdown() {
        //if we are in the middle of a partial copy push the data out, this is blocking
        while (0!=totalPrimaryCopy) {
            //if all the copies are done then record it as complete, does as much work as possible each time its called.
            copyToFile(this);
        }
    }
    
    private static <S extends MessageSchema> void processAvailData(TapeWriteStage<S> ss) {

        //only zero when we need to find the next block and have finished the previous
        if (0==ss.totalPrimaryCopy) {
            findStableCutPoint(ss);   
            
            //we have established the point that we can read up to, this value is changed by the writer on the other side
                                        
            //get the start and stop locations for the copy
            //now find the point to start reading from, this is moved forward with each new read.
            if ((ss.totalPrimaryCopy = (ss.headPos - ss.cachedTail)) <= 0) {                      
                assert(ss.totalPrimaryCopy==0);
                return; //nothing to copy so come back later
            }
            setupBuffersToWriteFrom(ss);              
        }
        copyToFile(ss);       
        
    }

    private static <S extends MessageSchema> void setupBuffersToWriteFrom(TapeWriteStage<S> ss) {
        //collect all the constant values needed for doing the copy
        ss.tempByteTail = Pipe.getBlobRingTailPosition(ss.source);
        ss.totalBytesCopy =   ss.byteHeadPos -ss.tempByteTail;
        ss.byteTailPos = ss.source.byteMask & ss.tempByteTail;   
        
        int blobLimitA = ss.source.sizeOfBlobRing;
        int blobPosition = ss.byteTailPos;
        int blobLimitB = (blobPosition+ss.totalBytesCopy);
        
        if (blobLimitB>blobLimitA) {
            ss.blobBuffer1.limit(blobLimitA);
            ss.blobBuffer2.limit(ss.source.byteMask & blobLimitB);
        } else {
            ss.blobBuffer1.limit(blobLimitB);
            ss.blobBuffer2.limit(        0  );                
        }
        ss.blobBuffer1.position(blobPosition);
        ss.blobBuffer2.position(            0 );
        
        
        int slabLimitA = ss.source.sizeOfSlabRing;
        int slabPosition = ss.source.mask & (int)ss.cachedTail;
        int slabLimitB =  (int)(slabPosition+ss.totalPrimaryCopy); 
        
        if (slabLimitB>slabLimitA) {
            ss.slabBuffer1.limit(slabLimitA);
            ss.slabBuffer2.limit(ss.source.mask & slabLimitB);
        } else {         
            ss.slabBuffer1.limit(slabLimitB);
            ss.slabBuffer2.limit(       0  );                
        }
        ss.slabBuffer1.position(slabPosition);
        ss.slabBuffer2.position(0);
                
        ss.headerInt.clear();
        ss.headerInt.put(ss.totalBytesCopy);
        ss.headerInt.put((int)ss.totalPrimaryCopy<<2);
        
        ss.header.clear();
    }

    private static <S extends MessageSchema> void copyToFile(TapeWriteStage<S> ss) {
        try {
            
            if (ss.header.hasRemaining()) {
                ss.fileChannel.write(ss.header);
                if (ss.header.hasRemaining()) {
                    return;
                }
            }
            
            if (ss.blobBuffer1.hasRemaining()) {
                ss.fileChannel.write(ss.blobBuffer1);
                if (ss.blobBuffer1.hasRemaining()) {
                    return;
                }
            }
            
            if (ss.blobBuffer2.hasRemaining()) {
                ss.fileChannel.write(ss.blobBuffer2);
                if (ss.blobBuffer2.hasRemaining()) {
                    return;
                }
            }
            
            if (ss.slabBuffer1.hasRemaining()) {
                fileChannelWrite(ss, ss.slabBuffer1);                
                if (ss.slabBuffer1.hasRemaining()) {
                    return;
                }
            }

            if (ss.slabBuffer2.hasRemaining()) {
                fileChannelWrite(ss, ss.slabBuffer2);                
                if (ss.slabBuffer2.hasRemaining()) {
                    return;
                } 
            }
        
            recordCopyComplete(ss);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
    }

    private static <S extends MessageSchema> void fileChannelWrite(TapeWriteStage<S> ss, IntBuffer slabBuffer)
            throws IOException {
        long pos = ss.fileChannel.position();
        pos += ss.fileChannel.transferFrom(ss.IBA.init(slabBuffer), pos, slabBuffer.remaining()<<2);
        ss.fileChannel.position(pos);
    }

    private static <S extends MessageSchema> void recordCopyComplete(TapeWriteStage<S> ss) {
        //release tail so data can be written
        
        int tempByteTail = ss.tempByteTail;
        int totalBytesCopy = ss.totalBytesCopy;
        
        int i = Pipe.BYTES_WRAP_MASK&(tempByteTail + totalBytesCopy);
        Pipe.setBytesWorkingTail(ss.source, i);
        Pipe.setBytesTail(ss.source, i);   
        Pipe.publishWorkingTailPosition(ss.source,(ss.cachedTail+=ss.totalPrimaryCopy));
        ss.totalPrimaryCopy = 0; //clear so next time we find the next block
    }



    private static <S extends MessageSchema> void findStableCutPoint(TapeWriteStage<S> ss) {
        ss.byteHeadPos = Pipe.bytesHeadPosition(ss.source);
        ss.headPos = Pipe.headPosition(ss.source);      
        while(ss.byteHeadPos != Pipe.bytesHeadPosition(ss.source) || ss.headPos != Pipe.headPosition(ss.source) ) {
            ss.byteHeadPos = Pipe.bytesHeadPosition(ss.source);
            ss.headPos = Pipe.headPosition(ss.source);
        }
    }

    public String toString() {
        return getClass().getSimpleName()+ " source content "+Pipe.contentRemaining(source);
    }
    
    private static class IntBufferAdapter implements ReadableByteChannel {

        IntBuffer sourceBuffer; 
        
        @Override
        public boolean isOpen() {
            return true;
        }

        public IntBufferAdapter init(IntBuffer buffer) {
            sourceBuffer = buffer;
            return this;
        }

        @Override
        public void close() throws IOException {            
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            
            int count = Math.min(dst.remaining()>>2,sourceBuffer.remaining());
            int i = count;
            while (--i>=0) {
                dst.putInt(sourceBuffer.get());
            }            
            return count<<2;
        }
        
    }
    
}
