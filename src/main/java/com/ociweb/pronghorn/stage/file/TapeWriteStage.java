package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.ReadableByteChannel;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 *
 * @author Nathan Tippy
 *
 */
public class TapeWriteStage<T extends MessageSchema> extends PronghornStage {

	private Pipe<T> source;
	private FileChannel fileChannel;
	
	//Header between each chunk must define 
	//           (4)    total bytes of the following block  (both primary and secondary together, to enable skipping)
	//           (4)    byte count of primary (this is always checked first for space when reading)
	
	private ByteBuffer header = ByteBuffer.allocate(8);
	private IntBuffer  headerAsInts = header.asIntBuffer();
	private IntBuferReadableByteChannel INT_BUFFER_WRAPPER = new IntBuferReadableByteChannel();
	
	public int moreToCopy=-2;
	
	public TapeWriteStage(GraphManager gm, Pipe<T> source, FileChannel fileChannel) {
		super(gm,source,NONE);
		
		//NOTE when writing ring ring size must be set to half the size of the reader to ensure there is no blocking.		
		//when reading if we ever have a block that is bigger than 1/2 ring size we can detect the failure on that end.
		//these restrictions are also in keeping with optimal usage of SSD/Spindle drives that prefer to read/write larger blocks
		//when consuming stage of the reading ring can pull as little as it wants but that first ring should be large for optimal IO.
		
		this.source = source;
        this.fileChannel = fileChannel;
	}
	
	
	@Override
	public void startup() {	
		try {
			fileChannel.position(fileChannel.size());//start at the end of the file so we always append to the end.
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	@Override
	public void shutdown() {
		try {
			fileChannel.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}		
	}


	@Override
	public void run() {		
		while (processAvailData(this)) {
			//keeps going while there is room to write or there is data to be written.
		}
	}

	private static <S extends MessageSchema> boolean processAvailData(TapeWriteStage<S> ss) {
		int byteHeadPos;
        long headPos;
		       
        //get the new head position
        byteHeadPos = Pipe.bytesHeadPosition(ss.source);
		headPos = Pipe.headPosition(ss.source);		
		while(byteHeadPos != Pipe.bytesHeadPosition(ss.source) || headPos != Pipe.headPosition(ss.source) ) {
			byteHeadPos = Pipe.bytesHeadPosition(ss.source);
			headPos = Pipe.headPosition(ss.source);
		}	
			
		
		//we have established the point that we can read up to, this value is changed by the writer on the other side
						
		//get the start and stop locations for the copy
		//now find the point to start reading from, this is moved forward with each new read.		
		int pMask = ss.source.mask;
		long tempTail = Pipe.tailPosition(ss.source);
		int primaryTailPos = pMask & (int)tempTail;				
		long totalPrimaryCopy = (headPos - tempTail);
		if (totalPrimaryCopy <= 0) {
			assert(totalPrimaryCopy==0);
			return false; //nothing to copy so come back later
		}
			
		int bMask = ss.source.byteMask;		
		int tempByteTail = Pipe.getBlobRingTailPosition(ss.source);
		int byteTailPos = bMask & tempByteTail;
		int totalBytesCopy =      (bMask & byteHeadPos) - byteTailPos; 
		if (totalBytesCopy < 0) {
			totalBytesCopy += (bMask+1);
		}
				
		//now do the copies
		if (0==totalBytesCopy || doingCopy(ss, byteTailPos, primaryTailPos, (int)totalPrimaryCopy, totalBytesCopy)) {

			//release tail so data can be written
			int i = Pipe.BYTES_WRAP_MASK&(tempByteTail + totalBytesCopy);
			Pipe.setBytesWorkingTail(ss.source, i);
			Pipe.setBytesTail(ss.source,i);		
			Pipe.publishWorkingTailPosition(ss.source,tempTail + totalPrimaryCopy);
			
			return true;
		}
		return false; //finished all the copy  for now
	}

	//single pass attempt to copy if any can not accept the data then they are skipped
	//and true will be returned instead of false.
	private static <S extends MessageSchema> boolean doingCopy(TapeWriteStage<S> ss, 
                                			                   int byteTailPos, 
                                			                   int primaryTailPos, 
                                			                   int totalPrimaryCopy, 
                                			                   int totalBytesCopy) {

		
		IntBuffer primaryInts = Pipe.wrappedStructuredLayoutRingBuffer(ss.source);
		ByteBuffer secondaryBytes = Pipe.wrappedUnstructuredLayoutRingBufferA(ss.source);

		primaryInts.position(primaryTailPos);
		primaryInts.limit(primaryTailPos+totalPrimaryCopy); //TODO: AA, this will not work on the wrap, we must mask and do muliple copies
		
		secondaryBytes.position(byteTailPos);
		secondaryBytes.limit(byteTailPos+totalBytesCopy);
				
		ss.header.clear();
		ss.headerAsInts.put(totalBytesCopy+(totalPrimaryCopy<<2)); //build own INT_BUFFER_WRAPPER ??
		ss.headerAsInts.put(totalPrimaryCopy<<2);
				
		
		try {
		    
		    //TODO: must return false if there is no room to write.
            long bytesWritten1 = ss.fileChannel.write(ss.header);
            long bytesWritten2 = ss.fileChannel.transferFrom(ss.INT_BUFFER_WRAPPER, 0, ss.INT_BUFFER_WRAPPER.init(primaryInts));
            long bytesWritten3 = ss.fileChannel.write(secondaryBytes);
            		
		} catch (IOException e) {
		    throw new RuntimeException(e);

        }
		
		
		
		
		//TODO: A, FileChannels are confirmed to be faster so this implementation needs to be updated.
		//      delete once the above is tested
//		MappedByteBuffer mapped;
//		try {
//			mapped = ss.fileChannel.map(MapMode.READ_WRITE, ss.fileChannel.position(), 8+totalBytesCopy+(totalPrimaryCopy<<2));
//			mapped.put(ss.header);
//			
//			IntBuffer asIntBuffer = mapped.asIntBuffer();
//			asIntBuffer.position(2);
//			asIntBuffer.put(primaryInts);
//			
//			mapped.position(mapped.position()+(totalPrimaryCopy<<2));
//			mapped.put(secondaryBytes);
//						
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		} 
		return true;
		
	}

	public String toString() {
		return getClass().getSimpleName()+ (-2==moreToCopy ? " not running ": " moreToCopy:"+moreToCopy)+" source content "+Pipe.contentRemaining(source);
	}
	
	//NOTE: this trick may be worth tweeting about.
	private class IntBuferReadableByteChannel implements ReadableByteChannel {

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
        public int read(ByteBuffer dst) throws IOException {
            if (!isOpen()) {
                return -1;
            }
            
            int chunkSize = Math.min( dst.remaining(), 
                                      buffer.remaining()*4);
            
            int i = (chunkSize>>2);// ints to be written
            while (--i>=0) {                                
                dst.putInt(buffer.get());
            }
            
            if (0==buffer.remaining()) {
                close();
            }
            
            return chunkSize;
        }	    
	}
}
