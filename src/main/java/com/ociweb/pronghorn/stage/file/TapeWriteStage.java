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
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Write data from a pipe directly to disk. This records both the slab and blob data without any schema concerns.
 * This is the simplest way to save data in a pipe to disk.
 * 
 * @author Nathan Tippy
 *
 */
public class TapeWriteStage<T extends MessageSchema<T>> extends PronghornStage {

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
    
    private static <S extends MessageSchema<S>> void processAvailData(TapeWriteStage<S> ss) {

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

    private static <S extends MessageSchema<S>> void setupBuffersToWriteFrom(TapeWriteStage<S> ss) {
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
        int slabPosition = ss.source.slabMask & (int)ss.cachedTail;
        int slabLimitB =  (int)(slabPosition+ss.totalPrimaryCopy); 
        
        if (slabLimitB>slabLimitA) {
            ss.slabBuffer1.limit(slabLimitA);
            ss.slabBuffer2.limit(ss.source.slabMask & slabLimitB);
        } else {         
            ss.slabBuffer1.limit(slabLimitB);
            ss.slabBuffer2.limit(       0  );                
        }
        ss.slabBuffer1.position(slabPosition);
        ss.slabBuffer2.position(0);
                
        ss.headerInt.clear();
        ss.headerInt.put(ss.totalBytesCopy);
        ss.headerInt.put((int)ss.totalPrimaryCopy<<2); //TODO: this value x4 is not right when we use packed values, TODO: how to determine this?
        
        ss.header.clear();
    }

    private static <S extends MessageSchema<S>> void copyToFile(TapeWriteStage<S> ss) {
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

    private static <S extends MessageSchema<S>> void fileChannelWrite(TapeWriteStage<S> ss, IntBuffer slabBuffer)
            throws IOException {
        long pos = ss.fileChannel.position();
        pos += ss.fileChannel.transferFrom(ss.IBA.init(slabBuffer), pos, slabBuffer.remaining()<<2);
        ss.fileChannel.position(pos);
    }

    private static <S extends MessageSchema<S>> void recordCopyComplete(TapeWriteStage<S> ss) {
        //release tail so data can be written
        
        int tempByteTail = ss.tempByteTail;
        int totalBytesCopy = ss.totalBytesCopy;
        
        int i = Pipe.BYTES_WRAP_MASK&(tempByteTail + totalBytesCopy);
        Pipe.setBytesWorkingTail(ss.source, i);
        Pipe.setBytesTail(ss.source, i);   
        Pipe.publishWorkingTailPosition(ss.source,(ss.cachedTail+=ss.totalPrimaryCopy));
        ss.totalPrimaryCopy = 0; //clear so next time we find the next block
    }



    private static <S extends MessageSchema<S>> void findStableCutPoint(TapeWriteStage<S> ss) {
        ss.byteHeadPos = Pipe.getBlobRingHeadPosition(ss.source);
        ss.headPos = Pipe.headPosition(ss.source);      
        while(ss.byteHeadPos != Pipe.getBlobRingHeadPosition(ss.source) || ss.headPos != Pipe.headPosition(ss.source) ) {
            ss.byteHeadPos = Pipe.getBlobRingHeadPosition(ss.source);
            ss.headPos = Pipe.headPosition(ss.source);
        }
    }
    
    private static class IntBufferAdapter implements ReadableByteChannel {

        IntBuffer sourceBuffer; 
        byte[] workspace = new byte[5];
        
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
            //byte[] localWorkspace = workspace;
            IntBuffer localSource = sourceBuffer;
            
            while (--i>=0) {
                
                //old  459ms  435MB/s    797ms  250MB/s
                dst.putInt(localSource.get());

                //TODO: new way works nice however we must go back and write the byte count now that it has been packed !! Retest and check speed.
                //new   Median Tape write duration: 354ms  564MB/s
//                int value = localSource.get();
//                int mask = (value>>31);         // FFFFF  or 000000
//                int check = (mask^value)-mask;  //absolute value
//                int bit = (int)(check>>>31);     //is this the special value?
//                //writeIntUnified(value, (check>>>bit)+bit, dst, (byte)0x7F);
//                
//                int len = writeIntUnified(value, (check>>>bit)+bit, localWorkspace, 0, (byte)0x7F);
//                dst.put(localWorkspace,  0,  len);
                
                
            }            
            return count<<2;
        }
        
        private static final int writeIntUnified(final int value,final int check, final byte[] buf, int pos, final byte low7) {
            
            if (check < 0x0000000000000040) {
            } else {
                if (check < 0x0000000000002000) {
                } else {
                    if (check < 0x0000000000100000) {
                    } else {
                        if (check < 0x0000000008000000) {
                        } else {                        
                            buf[pos++] = (byte) (((value >>> 28) & low7));
                        }
                        buf[pos++] = (byte) (((value >>> 21) & low7));
                    }
                    buf[pos++] = (byte) (((value >>> 14) & low7));
                }
                buf[pos++] = (byte) (((value >>> 7) & low7));
            }
            buf[pos++] = (byte) (((value & low7) | 0x80));
            return pos;
        }
        
    }
    
}
