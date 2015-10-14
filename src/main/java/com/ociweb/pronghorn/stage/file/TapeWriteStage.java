package com.ociweb.pronghorn.stage.file;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.ReadableByteChannel;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 *
 * @author Nathan Tippy
 *
 */
public class TapeWriteStage extends PronghornStage {

	private Pipe<RawDataSchema> sourcePipe;
	private FileChannel fileChannel;
	
	//Header between each chunk must define 
	//           (4)    total bytes of the following block  (both primary and secondary together, to enable skipping)
	//           (4)    byte count of primary (this is always checked first for space when reading)
	
	private IntBuferReadableByteChannel INT_BUFFER_WRAPPER = new IntBuferReadableByteChannel();
	private HeaderReadableByteChannel   HEADER_WRAPER = new HeaderReadableByteChannel();
	
	public int moreToCopy=-2;
	private final RandomAccessFile outputFile;
	
	///TODO: add second pipe with commands eg (write N fields to X file then close)
	
	public TapeWriteStage(GraphManager gm, Pipe<RawDataSchema> input, RandomAccessFile outputFile) {
		super(gm,input,NONE);
		
		//TODO: Add command pipe to write so many then change channels etc.
		
		//NOTE when writing ring ring size must be set to half the size of the reader to ensure there is no blocking.		
		//when reading if we ever have a block that is bigger than 1/2 ring size we can detect the failure on that end.
		//these restrictions are also in keeping with optimal usage of SSD/Spindle drives that prefer to read/write larger blocks
		//when consuming stage of the reading ring can pull as little as it wants but that first ring should be large for optimal IO.
		
		this.sourcePipe = input;
        this.outputFile = outputFile;
	}
	
	
	@Override
	public void startup() {	
		try {
		    fileChannel = outputFile.getChannel();
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

	private static boolean processAvailData(TapeWriteStage ss) {
		int byteHeadPos;
        long headPos;
		       
        //get the new head position
        byteHeadPos = Pipe.bytesHeadPosition(ss.sourcePipe);
		headPos = Pipe.headPosition(ss.sourcePipe);		
		while(byteHeadPos != Pipe.bytesHeadPosition(ss.sourcePipe) || headPos != Pipe.headPosition(ss.sourcePipe) ) {
			byteHeadPos = Pipe.bytesHeadPosition(ss.sourcePipe);
			headPos = Pipe.headPosition(ss.sourcePipe);
		}	
			
		
		//we have established the point that we can read up to, this value is changed by the writer on the other side
						
		//get the start and stop locations for the copy
		//now find the point to start reading from, this is moved forward with each new read.		
		int pMask = ss.sourcePipe.mask;
		long tempTail = Pipe.tailPosition(ss.sourcePipe);
		int primaryTailPos = pMask & (int)tempTail;				
		long totalPrimaryCopy = (headPos - tempTail);
		if (totalPrimaryCopy <= 0) {
			assert(totalPrimaryCopy==0);
			return false; //nothing to copy so come back later
		}
			
		int bMask = ss.sourcePipe.byteMask;		
		int tempByteTail = Pipe.getBlobRingTailPosition(ss.sourcePipe);
		int byteTailPos = bMask & tempByteTail;
		int totalBytesCopy =      (bMask & byteHeadPos) - byteTailPos; 
		if (totalBytesCopy < 0) {
			totalBytesCopy += (bMask+1);
		}
				
		//now do the copies
		if (0==totalBytesCopy || doingCopy(ss, byteTailPos, primaryTailPos, (int)totalPrimaryCopy, totalBytesCopy)) {

			//release tail so data can be written
			int i = Pipe.BYTES_WRAP_MASK&(tempByteTail + totalBytesCopy);
			Pipe.setBytesWorkingTail(ss.sourcePipe, i);
			Pipe.setBytesTail(ss.sourcePipe,i);		
			Pipe.publishWorkingTailPosition(ss.sourcePipe,tempTail + totalPrimaryCopy);
			
			return true;
		}
		return false; //finished all the copy  for now
	}

	//single pass attempt to copy if any can not accept the data then they are skipped
	//and true will be returned instead of false.
	private static boolean doingCopy(TapeWriteStage ss, 
                                          int byteTailPos, 
            			                   int primaryTailPos, 
            			                   int totalPrimaryCopy, 
            			                   int totalBytesCopy) {

	    
	     
	    
	    
	    
	    
	    //TODO: ensure this block is reentrant ok.
		
	    
		IntBuffer primaryInts = Pipe.wrappedSlabRing(ss.sourcePipe);
		ByteBuffer secondaryBytes = Pipe.wrappedBlobRingA(ss.sourcePipe);
		primaryInts.position(primaryTailPos);
		int newLimit = primaryTailPos+totalPrimaryCopy;
		
		
		//ss.sourcePipe.mask
		
        primaryInts.limit(newLimit); //TODO: AA, this will not work on the wrap, we must mask and do muliple copies
		
		secondaryBytes.position(byteTailPos);
		secondaryBytes.limit(byteTailPos+totalBytesCopy);//TODO: what about wrapped bytes? we need ringB

		ss.HEADER_WRAPER.init(totalBytesCopy+(totalPrimaryCopy<<2), totalPrimaryCopy<<2);
		int slabCount = ss.INT_BUFFER_WRAPPER.init(primaryInts);
				
		
		try {
		    
		    //TODO: must return false if there is no room to write.
            long bytesWritten1 = ss.fileChannel.transferFrom(ss.HEADER_WRAPER, 0, 8);
            
            if (bytesWritten1<0) {
                //unexpected end of file
                return false;
            }
            if (0==bytesWritten1) {
                //try again later;
                return false;
            }
            
            
            
            long bytesWritten2 = ss.fileChannel.transferFrom(ss.INT_BUFFER_WRAPPER, 0, slabCount);
            
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
		return getClass().getSimpleName()+ (-2==moreToCopy ? " not running ": " moreToCopy:"+moreToCopy)+" source content "+Pipe.contentRemaining(sourcePipe);
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
	
	private class HeaderReadableByteChannel implements ReadableByteChannel {

	    private boolean isOpen;
	    private int a;
	    private int b;
	    
	    public int init(int a, int b) {
	        this.isOpen = true;
	        this.a = a;
	        this.b = b;
	        return 8;
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
        public int read(ByteBuffer dst) throws IOException {
            if (!isOpen()) {
                return -1;
            }
            //important to ensure atomic write of this full block
            if (dst.remaining()<8) {
                return 0;
            }
            dst.putInt(a);
            dst.putInt(b);
            close();
            return 8;
            
        }
	    
        
	}
	
	
}
