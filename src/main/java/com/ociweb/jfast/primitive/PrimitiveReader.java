//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import java.io.IOException;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.jfast.util.Profile;

/**
 * PrimitiveReader
 * 
 * Must be final and not implement any interface or be abstract. In-lining the
 * methods of this class provides much of the performance needed by
 * this library.
 * 
 * This class also has no member methods, all the methods are static.  This allows 
 * the Java and Julia implementations to have the same design.  It also removes any vtable
 * lookups that may have been required by a more traditional approach.
 * 
 * None of the Optional field logic is here.  That can be better optimized at the layer above
 * this class. Here we will find Unsigned/Signed, Char sequence and Byte sequence logic. How 
 * these methods are combined in order to meet the needs of the spec are found in the 
 * FASTReaderDispatchTemplates class.
 * 
 * @author Nathan Tippy
 * 
 */

public final class PrimitiveReader {

    // Note: only the type/opp combos used will get in-lined, this small
    // footprint will fit in execution cache.
    // if we in-line too much the block will be to large and may spill.

    private final int resetLimit;  
    private final FASTInput input;
    public final byte[] buffer;
    
    private long totalReader;
    
    private byte[] invPmapStack;
    private int invPmapStackDepth;

    public int position; 
    public int limit;
    
    public int pmapIdxBitBlock = -1; //idx high in pmap data
    
    
    private InputBlockagePolicy blockagePolicy = new InputBlockagePolicy(){ //   blockagePolicy
        
        @Override
        public void detectedInputBlockage(int need, FASTInput input) {
        }

        @Override
        public void resolvedInputBlockage(FASTInput input) {
        }};
  
     //only called when we need more data and the input is not providing any
     //TODO: B, this will be refactored again as we add "reactive streams" into this implementation   
     public void setInputPolicy(InputBlockagePolicy is) {
       blockagePolicy = is; //TODO: B, this needs to be specific for this instance?
     }
        
    
	public static void printDebugData(PrimitiveReader reader) {
		int pos = reader.position;
		int lim = Math.min(reader.limit, pos+70);
		
		long base = reader.totalReader-(reader.limit-reader.position)-reader.position;
		System.err.println("printing details of bytes "+(base+pos)+" up to "+(base+lim));
		while (pos<lim) {
			
			String temp = "00000000"+Integer.toBinaryString(reader.buffer[pos]);    		
			System.err.println((base+pos)+" data:"+ temp.substring(temp.length()-8)+"    "+reader.buffer[pos] );
			
			pos++;
		}
				
				
		
	}


	/**
     * 
     * Making the bufferSize large will decrease the number of copies but may increase latency.
     * Making the bufferSize small will decrease latency but may reduce overall throughput.
     * 
     * @param bufferSizeInBytes must be large enough to hold a single group
     * @param input
     * @param maxPMapCountInBytes must be large enough to hold deepest possible nesting of pmaps
     */
    public PrimitiveReader(int bufferSizeInBytes, FASTInput input, int maxPMapCountInBytes) { 
        this.input = input;
        this.buffer = new byte[bufferSizeInBytes];
        this.resetLimit = 0;
        this.position = 0;
        this.limit = 0;        
        this.invPmapStack = new byte[maxPMapCountInBytes];//need trailing bytes to avoid conditional when using.
        this.invPmapStackDepth = maxPMapCountInBytes-2;
        input.init(this.buffer);
    }
    
    public PrimitiveReader(byte[] buffer) {
        this.input = null; //TODO: C, may want dummy impl for this.
        this.buffer = buffer;
        this.resetLimit = buffer.length;
        
        this.position = 0;
        this.limit = buffer.length;
        //in this case where the full data is provided then we know it can not be larger than the buffer.
        int maxPMapCountInBytes = buffer.length+6;
        this.invPmapStack = new byte[maxPMapCountInBytes];//need trailing bytes to avoid conditional when using.
        this.invPmapStackDepth = maxPMapCountInBytes-2;

    }
    
    public PrimitiveReader(byte[] buffer, int maxPMapCountInBytes) {
        this.input = null; //TODO: C, may want dummy impl for this.
        this.buffer = buffer;
        this.resetLimit = buffer.length;
        
        this.position = 0;
        this.limit = buffer.length;
        this.invPmapStack = new byte[maxPMapCountInBytes];//need trailing bytes to avoid conditional when using.
        this.invPmapStackDepth = maxPMapCountInBytes-2;

    }

    /**
     * Reset the internal state to its initial values.
     * 
     * This is needed when re-using the reader expcially by unit tests.
     * @param reader
     */
    public static final void reset(PrimitiveReader reader) {
        reader.totalReader = 0;
        reader.position = 0;
        reader.limit = reader.resetLimit;
        reader.pmapIdxBitBlock = -1;
        reader.invPmapStackDepth = reader.invPmapStack.length - 2;

    }
    
    /**
     * Returns the total number of bytes that have been taken from the input. 
     * 
     * @param reader
     * @return
     */
    public static final long totalRead(PrimitiveReader reader) {
        return reader.totalReader;
    }

    /**
     * Returns the total number of bytes that are currently waiting in the buffer to be parsed.
     * 
     * Using this with the totalRead method allows the caller to compute the exact byte positions being parsed.
     * 
     * @param reader
     * @return
     */
    public static final int bytesReadyToParse(PrimitiveReader reader) {
        return reader.limit - reader.position;
    }

    /**
     * Non blocking call that will pull more data from the input if the internal
     * buffer has room at the source has data available.
     * 
     * @param reader
     */
    public static final void fetch(PrimitiveReader reader) {
        fetch(0, reader);
    }
    
    /**
     *  Blocking call will not return until the need is met because the parser has
     *  determined that we can not continue until this data is provided.
     *  this call may however read in more than the need because its ready
     *  and convenient to reduce future calls.
     *  
     * @param need
     * @param reader
     */
    private static void fetch(int need, PrimitiveReader reader) {

        need = fetchAvail(need, reader);        
        if (need > 0) {     
            reader.blockagePolicy.detectedInputBlockage(need, reader.input);
            if (reader.limit+need > reader.buffer.length) {
            	throw new FASTException("buffer for PrimitiveReader is too small");
            }
            int filled = reader.input.blockingFill(reader.limit, need);
            reader.blockagePolicy.resolvedInputBlockage(reader.input);
            reader.totalReader += filled;
            reader.limit += filled;
            if (filled<need) {
                throw new FASTException("Unexpected end of data.");
            }
        }
    }
    
    private static int fetchAvail(int need, PrimitiveReader reader) {
        if (reader.position >= reader.limit) {
            reader.position = reader.limit = 0;
          //  System.err.println("reset fetchAvail");
        }
        int remainingSpace = reader.buffer.length - reader.limit;
        if (need <= remainingSpace) {
            // fill remaining space if possible to reduce fetch later
            // but as we near the end prevent overflow by only getting what is needed.
            
            int filled = reader.input.fill(reader.limit, remainingSpace);
     //       System.err.println("fill from "+reader.limit+" xx "+remainingSpace+" filled "+filled+" pos "+reader.position);

            reader.totalReader += filled;
            reader.limit += filled;
            //
            return need - filled;
        } else {
            return noRoomOnFetch(need, reader);
        }
    }
    
    /**
     * Move all the data in the buffer down so we have room for the new data.
     * This call must be minimized because it can cause a spike it does however allow the normal
     * case to go faster without having the deal with ring buffer logic.
     * 
     * @param need
     * @param reader
     * @return
     */
    private static int noRoomOnFetch(int need, PrimitiveReader reader) {
     //   System.err.println("____ no room on fetch need "+need+" after  pos: "+reader.position+" lim:"+reader.limit);
        
        int keepFromPosition = reader.position;
        
        // not enough room at end of buffer for the need
        int populated = reader.limit - keepFromPosition;
     //   int reqiredSize = need + populated;

//        assert (reader.buffer.length >= reqiredSize) : "internal buffer is not large enough, requres " + reqiredSize
//                + " bytes";
        
        System.arraycopy(reader.buffer, keepFromPosition, reader.buffer, 0, populated);
        // if possible fill
        int filled = reader.input.fill(populated, reader.buffer.length - populated);

        reader.position = 0;
        reader.totalReader += filled;
        reader.limit = populated + filled;
        
    //    System.err.println("new pos:"+reader.position+" lim:"+reader.limit+" filled:"+filled);

        return need - filled;

    }



    // ///////////////
    // pmapStructure
    // 1 2 3 4 5 D ? I 2 3 4 X X
    // 0 0 0 0 1 D ? I 0 0 1 X X
    //
    // D delta to last position
    // I pmapIdx of last stack frame
    // //
    // called at the start of each group unless group knows it has no pmap
    
    /**
     * Begin/Open a new PMap.  This is should be done for the begging of the messages or groups
     * that require a PMap based on the used operators within that group.
     * 
     * @param pmapMaxSize the PMap is known to never be larger than this
     * @param reader
     */
    
    public static final void openPMap(final int pmapMaxSize, PrimitiveReader reader) {

        //TODO: B, pmapMaxSize is a constant for many templates and can be injected.
        // set next bit to read
        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        
        // push the old index for resume
        reader.invPmapStack[reader.invPmapStackDepth - 1] = (byte) (reader.pmapIdxBitBlock>>16);  //reader.pmapIdx;

        int k = reader.invPmapStackDepth -= (pmapMaxSize + 2);
//        if (k<0) {
//            new Exception("bad k value "+k).printStackTrace();
//            System.exit(0);
//        }

        reader.pmapIdxBitBlock = (6<<16)|(0xFF&reader.buffer[reader.position]);
        
        k = walkPMapLength(pmapMaxSize, k, reader.invPmapStack, reader, reader.buffer);
        reader.invPmapStack[k] = (byte) (3 + pmapMaxSize + (reader.invPmapStackDepth - k));


    }

    private static int walkPMapLength(final int pmapMaxSize, int k, byte[] pmapStack, PrimitiveReader reader, byte[] buffer) {
        int rp = reader.position;
        if (reader.limit - rp >= pmapMaxSize) {
            if ((pmapStack[k++] = buffer[rp++]) >= 0) {
                if ((pmapStack[k++] = buffer[rp++]) >= 0) {
                	try{
	                    do {
	                    } while ((pmapStack[k++] = buffer[rp++]) >= 0);
                	} catch (ArrayIndexOutOfBoundsException outOfBounds) {
                		throw new FASTException("No high bit set to mark the end of the PMAP");
                	}
                }
            }
            reader.position = rp;
        } else {
            k = openPMapSlow(k,reader, buffer);
        }
        return k;
    }

    private static int openPMapSlow(int k, PrimitiveReader reader, byte[] buffer) {

        // must use slow path because we are near the end of the buffer.
        while ((reader.invPmapStack[k++] = buffer[reader.position++]) >= 0) {
            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
        }
        return k;
    }

    //NOTE: for consistancy and to help with branch prediction ALWAYS check this against zero unless using brancheless
    
    /**
     * Read the next PMap bit from the open PMap. The cursor is moved forward so another call will read the next bit.
     * 
     * @param reader
     * @return
     */
    public static byte readPMapBit(PrimitiveReader reader) { 
            if (reader.pmapIdxBitBlock >= 0 ) {    
			        // Frequent, 6 out of every 7 plus the last bit block 
			        int shft = reader.pmapIdxBitBlock>>16;
			        reader.pmapIdxBitBlock -= (1<<16);                
			        return (byte) (1 & (reader.pmapIdxBitBlock >>> shft)); 
			} else {   
			    return readPMapBitNextByte(reader);               
			}
    }


	//needed for code generation to eliminate conditional to detect end of 7 bits
    /**
     * Reads the next PMap and moves to the next byte position in the PMap.
     * 
     * @param reader
     * @return
     */
    public static byte readPMapBitNextByte(PrimitiveReader reader) {
    	
    	
        if (((byte)(0xFF&reader.pmapIdxBitBlock)) < 0 ) {
            reader.pmapIdxBitBlock = (6<<16)|0x80;
            return 0;
        } else {
            int tmp = (5<<16)|(0xFF&reader.invPmapStack[++reader.invPmapStackDepth]);
            reader.pmapIdxBitBlock = tmp;                           
            return (byte) (1 & (tmp >>> 6));
        }
    }

    // called at the end of each group
    
    /**
     * Close the open PMap.  Should only call when the message or group has opened a PMap
     *   
     * @param reader
     */
    public static final void closePMap(PrimitiveReader reader) {
        byte bitBlock = reader.invPmapStack[reader.invPmapStackDepth += (reader.invPmapStack[reader.invPmapStackDepth + 1])];
        reader.pmapIdxBitBlock = (reader.invPmapStack[reader.invPmapStackDepth - 1]<<16)|bitBlock;        
    }

    // ///////////////////////////////////
    // ///////////////////////////////////
    // ///////////////////////////////////

    //recursive use of the stack turns out to be a good way to unroll this loop.
    private static long readLongSignedTail(long a, PrimitiveReader reader) {
        byte v = reader.buffer[reader.position++];
        return (v<0) ? a | (v & 0x7Fl) : readLongSignedTail((a | v) << 7,reader);
    }
    
    
    /**
     * Parse a 64 bit signed value from the buffer
     * 
     * @param reader
     * @return
     */
    public static long readLongSigned(PrimitiveReader reader) {   
        int rp = reader.position;
        if (reader.limit - rp >= 10) {// not near end so go fast.
            byte v = reader.buffer[rp];        
            reader.position = rp+1;
            long accumulator = (~((long)(((v>>6)&1)-1)))&0xFFFFFFFFFFFFFF80l; 
            return (v < 0) ? accumulator |(v & 0x7F) : readLongSignedTail((accumulator | v) << 7,reader);
        }
        return readLongSignedSlow(reader);
    }

    /**
     * Slow version of 64 bit parser that is used when the position is near the end of the buffer.
     * 
     * @param reader
     * @return
     */
    private static long readLongSignedSlow(PrimitiveReader reader) {
        // slow path
        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        int v = reader.buffer[reader.position++];
        long accumulator = ((v & 0x40) == 0) ? 0 : 0xFFFFFFFFFFFFFF80l;

        while (v >= 0) { // (v & 0x80)==0) {
            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
            accumulator = (accumulator | v) << 7;
            v = reader.buffer[reader.position++];
        }
        return accumulator | (v & 0x7F);
    }

    //TODO: X, a lot of research went into this under 6 & 7, try this a a simple loop and re-test both approaches on Java 8
    //recursive use of the stack turns out to be a good way to unroll this loop.
    private static long readLongUnsignedTail(long a, PrimitiveReader reader) {
        byte v = reader.buffer[reader.position++];
        return (v<0) ? (a << 7) | (v & 0x7F) : readLongUnsignedTail((a<<7)|v,reader);
    }
    
    
    /**
     * Parse 64 bit unsigned value from the buffer
     * 
     * @param reader
     * @return
     */
    public static long readLongUnsigned(PrimitiveReader reader) {
        
        if (reader.limit - reader.position >= 10) {// not near end so go fast.
            byte v = reader.buffer[reader.position++];
            return (v < 0) ? (v & 0x7F) : readLongUnsignedTail(v,reader);
        }
        return readLongUnsignedSlow(reader);
    }

    private static long readLongUnsignedSlow(PrimitiveReader reader) {
        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        byte v = reader.buffer[reader.position++];
        long accumulator;
        if (v >= 0) { // (v & 0x80)==0) {
            accumulator = v << 7;
        } else {
            return (v & 0x7F);
        }

        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        v = reader.buffer[reader.position++];

        while (v >= 0) { // (v & 0x80)==0) {
            accumulator = (accumulator | v) << 7;

            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
            v = reader.buffer[reader.position++];

        }
        return accumulator | (v & 0x7F);
    }  
    
    //recursive use of the stack turns out to be a good way to unroll this loop.
    private static int readIntegerSignedTail(int a, PrimitiveReader reader) {
        byte v = reader.buffer[reader.position++];
        return (v<0) ? a | (v & 0x7F) : readIntegerSignedTail((a | v) << 7,reader);
    }
    
    /**
     * Parse a 32 bit signed integer from the buffer.
     * 
     */
    public static int readIntegerSigned(PrimitiveReader reader) {
    	  if (reader.limit - reader.position >= 10) {// not near end so go fast.
		    byte v = reader.buffer[reader.position++];
		 //   int accumulator = ((v & 0x40) == 0) ? 0 : 0xFFFFFF80;         
		    int accumulator = (~(((v>>6)&1)-1))&0xFFFFFF80;  //branchless                
		    return (v < 0) ? accumulator |(v & 0x7F) : readIntegerSignedTail((accumulator | v) << 7,reader);
		}
		return readIntegerSignedSlow(reader);
    }


	private static int readIntegerSignedSlow(PrimitiveReader reader) {
        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        byte v = reader.buffer[reader.position++];
        int accumulator = ((v & 0x40) == 0) ? 0 : 0xFFFFFF80;

        while (v >= 0) { // (v & 0x80)==0) {
            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
            accumulator = (accumulator | v) << 7;
            v = reader.buffer[reader.position++];
        }
        return accumulator | (v & 0x7F);
    }

    public static int readIntegerUnsigned(PrimitiveReader reader) {//Invoked 100's of millions of times, must be tight.
        int rp = reader.position;
        if (reader.limit - rp >= 5) {// not near end so go fast.
            byte v = reader.buffer[rp];
            reader.position = rp+1;
            return (v < 0) ? (v & 0x7F) : readIntegerUnsignedTail(v,reader);
        } else {
            return readIntegerUnsignedSlow(reader);
        }
    }

    //TODO: C, add overflow flag to support optional int that is outside 32 bits. Without this we dont quite match the spec.
    
    //recursive use of the stack turns out to be a good way to unroll this loop.
    private static int readIntegerUnsignedTail(int a, PrimitiveReader reader) {
        byte v = reader.buffer[reader.position++];
        return (v<0) ? (a << 7) | (v & 0x7F) : readIntegerUnsignedTail((a<<7)|v,reader);
    }
    
    private static int readIntegerUnsignedSlow(PrimitiveReader reader) {
        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        byte v = reader.buffer[reader.position++];
        int accumulator;
        if (v >= 0) { // (v & 0x80)==0) {
            accumulator = v << 7;
        } else {
            return (v & 0x7F);
        }

        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        v = reader.buffer[reader.position++];

        while (v >= 0) { // (v & 0x80)==0) {
            accumulator = (accumulator | v) << 7;
            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
            v = reader.buffer[reader.position++];
        }
        return accumulator | (v & 0x7F);
    }

    public static Appendable readTextASCII(Appendable target, PrimitiveReader reader) {
        if (reader.limit - reader.position < 2) {
            fetch(2, reader);
        }

        byte v = reader.buffer[reader.position];

        if (0 == v) {
            v = reader.buffer[reader.position + 1];
            if (0x80 != (v & 0xFF)) {
                throw new UnsupportedOperationException();
            }
            // nothing to change in the target
            reader.position += 2;
        } else {
            // must use count because the base of position will be in motion.
            // however the position can not be incremented or fetch may drop
            // data.

            while (reader.buffer[reader.position] >= 0) {
                try {
                    target.append((char) (reader.buffer[reader.position]));
                } catch (IOException e) {
                    throw new FASTException(e);
                }
                reader.position++;
                if (reader.position >= reader.limit) {
                    fetch(1, reader); // CAUTION: may change value of position
                }
            }
            try {
                target.append((char) (0x7F & reader.buffer[reader.position]));
            } catch (IOException e) {
                throw new FASTException(e);
            }

            reader.position++;

        }
        return target;
    }

    public static final int readTextASCIIIntoRing(byte[] target, int targetOffset, int mask, PrimitiveReader reader) {

        int rp = reader.position;
        if (reader.limit - rp > mask) {
            byte v = reader.buffer[rp];
            if (v < 0) {//single char
                target[mask & targetOffset] = (byte) (0x7F & v);            
                reader.position=rp+1;
                return 1;
            } else {
                return (v==0) ? readTextASCIIZero(reader) : readTextASCIINormal(target, targetOffset, mask, reader, v);
            }
        }        
        return readTextASCIIIntoRingSlow(target, targetOffset, mask, reader);
    }


    private static int readTextASCIINormal(byte[] target, int targetOffset, int mask, PrimitiveReader reader, byte v) {
        int p = reader.position;
        byte[] buffer = reader.buffer;
        int idx = targetOffset;
        while (v >= 0) {
            target[mask&idx++] = (byte) (buffer[p++]);
            v= buffer[p];
        }
        target[mask&idx++] = (byte) (0x7F & v);
               // System.err.println(p-reader.position);
        reader.position = p+1;
        return idx - targetOffset;// length of string
    }

    private static int readTextASCIIZero(PrimitiveReader reader) {
        int p = reader.position;
        byte[] buffer = reader.buffer;
        byte v;
        v = buffer[p + 1];
        if (0x80 != (v & 0xFF)) {
            throw new UnsupportedOperationException();
        }
        // nothing to change in the target
        reader.position += 2;
        return 0; // zero length string
    }

    private static int readTextASCIIIntoRingSlow(byte[] target, int targetOffset, int mask, PrimitiveReader reader) {
        if (reader.limit - reader.position < 2) {
            fetch(2, reader);
        }

        byte v = reader.buffer[reader.position];

        if (0 == v) {
            v = reader.buffer[reader.position + 1];
            if (0x80 != (v & 0xFF)) {
                throw new UnsupportedOperationException();
            }
            // nothing to change in the target
            reader.position += 2;
            return 0; // zero length string
        } else {
            int idx = targetOffset;
            while (v >= 0) {
                target[mask&idx++] = (byte) (reader.buffer[reader.position++]);
                if (reader.position >= reader.limit) {
                    fetch(1, reader); // CAUTION: may change value of position
                }
                v= reader.buffer[reader.position];
            }
            target[mask&idx++] = (byte) (0x7F & v); 
            reader.position++;
            return idx - targetOffset;// length of string

        }
    }
    
    
    public static final void readByteData(byte[] target, int offset, int length, PrimitiveReader reader) {
    	    	
        // ensure all the bytes are in the buffer before calling visitor
        if (reader.limit - reader.position < length) {
            fetch(length, reader);
            
            //safe place to do sanity check
            if (reader.limit - reader.position < length) {            	
            	//System.err.println(reader.limit +"  wbl:"+reader.position+" length:"+length+"   post fetch ");
            	throw new ArrayIndexOutOfBoundsException(length);
            }
            
        }
        //System.err.println("reading length:"+length+" from "+offset+" "+target.length+"  "+reader.position);
        System.arraycopy(reader.buffer, reader.position, target, offset, length);
        reader.position += length;
    }
    
    public static final int readTextASCII(byte[] target, int targetOffset, int targetLimit, PrimitiveReader reader) {

        int countDown = targetLimit - targetOffset;
        if (reader.limit - reader.position >= countDown) {
            // System.err.println("fast");
            // must use count because the base of position will be in motion.
            // however the position can not be incremented or fetch may drop
            // data.
            int idx = targetOffset;
            while (reader.buffer[reader.position] >= 0 && --countDown >= 0) {
                target[idx++] = (byte) (reader.buffer[reader.position++]);
            }
            if (--countDown >= 0) {
                target[idx++] = (byte) (0x7F & reader.buffer[reader.position++]);
                return idx - targetOffset;// length of string
            } else {
                return targetOffset - idx;// neg length of string if hit max
            }
        } else {
            return readAsciiText2Slow(target, targetOffset, countDown, reader);
        }
    }

    private static int readAsciiText2Slow(byte[] target, int targetOffset, int countDown, PrimitiveReader reader) {
        if (reader.limit - reader.position < 2) {
            fetch(2, reader);
        }

        // must use count because the base of position will be in motion.
        // however the position can not be incremented or fetch may drop data.
        int idx = targetOffset;
        while (reader.buffer[reader.position] >= 0 && --countDown >= 0) {
            target[idx++] = (byte) (reader.buffer[reader.position++]);
            if (reader.position >= reader.limit) {
                fetch(1, reader); // CAUTION: may change value of position
            }
        }
        if (--countDown >= 0) {
            target[idx++] = (byte) (0x7F & reader.buffer[reader.position++]);
            return idx - targetOffset;// length of string
        } else {
            return targetOffset - idx;// neg length of string if hit max
        }
    }

    // keep calling while byte is >=0
    public static final byte readTextASCIIByte(PrimitiveReader reader) {
        if (reader.position >= reader.limit) {
            fetch(1, reader); // CAUTION: may change value of position
        }
        return reader.buffer[reader.position++];
    }
    
    public static final void readSkipByStop(PrimitiveReader reader) {
        if (reader.position >= reader.limit) {
            fetch(1, reader);
        }
        while (reader.buffer[reader.position++] >= 0) {
            if (reader.position >= reader.limit) {
                fetch(1, reader);
            }
        }
    }

    public static final void readSkipByLengthByt(int len, PrimitiveReader reader) {
        if (reader.limit - reader.position < len) {
            fetch(len, reader);
        }
        reader.position += len;
    }






    public static final boolean isEOF(PrimitiveReader reader) {
        if (reader.limit != reader.position) {
            return false;
        }
        if (null==reader.input) {
            return true;
        }
        fetch(0, reader);
        return reader.limit != reader.position ? false : reader.input.isEOF();
    }


    // //////////////
    // /////////

    //only needed for preamble, which is not BTW found in the spec.
    public static int readRawInt(PrimitiveReader reader) {
        if (reader.limit-reader.position <4) {
            fetch(4, reader);
        }
        
        return (((0xFF & reader.buffer[reader.position++]) << 0) | 
                ((0xFF & reader.buffer[reader.position++]) << 8) |
                ((0xFF & reader.buffer[reader.position++]) << 16) | 
                ((0xFF & reader.buffer[reader.position++]) << 24));

    }


}
