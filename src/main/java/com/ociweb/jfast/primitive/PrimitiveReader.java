package com.ociweb.jfast.primitive;

import com.ociweb.jfast.ByteConsumer;
import com.ociweb.jfast.MyCharSequnce;
import com.ociweb.jfast.field.string.CharSequenceShadow;

/**
 * PrimitiveReader
 * 
 * Must be final and not implement any interface or be abstract.
 * In-lining the primitive methods of this class provides much
 * of the performance needed by this library.
 * 
 * 
 * @author Nathan Tippy
 *
 */

public final class PrimitiveReader {

	//TODO: must add skip bytes methods
	
	private final FASTInput input;
	
	private byte[] buffer; //TODO: build an Unsafe version of Reader and Writer for fastest performance on server.
	private int position;
	private int limit;
	private long totalReader;
	public static final int VERY_LONG_STRING_MASK = 0x7F; 
	
	
	public PrimitiveReader(FASTInput input) {
		this(4096,input);
	}
	
	//TODO: extract buffer wrapper so unsafe can be injected here.
	public PrimitiveReader(int initBufferSize, FASTInput input) {
		this.input = input;
		this.buffer = new byte[initBufferSize];
		this.position = 0;
		this.limit = 0;
	}
	
	public long totalRead() {
		return totalReader;
	}
	
	private final void fetch(int need) {
		//System.err.println("need more");
		if (position >= limit) {
			position = limit = 0;
		}
		int remainingSpace = buffer.length-limit;
		if (need<=remainingSpace) {	
			//fill remaining space if possible to reduce fetch later
			int filled = input.fill(buffer, limit, remainingSpace);
			totalReader+=filled;
			limit += filled;
		} else {
			noRoomOnFetch(need);
		}
	}

	private void noRoomOnFetch(int need) {
		//not enough room at end of buffer for the need
		int populated = limit - position;
		int reqiredSize = need + populated;
		if (buffer.length<reqiredSize) {
			grow(populated, reqiredSize);
		} else {
			System.arraycopy(buffer, position, buffer, 0, populated);
		}
		//fill and return
		int filled = input.fill(buffer, populated, buffer.length - populated);
		totalReader+=filled;
		position = 0;
		limit = populated+filled;
	}

	private void grow(int populated, int reqiredSize) {
		
		//grow buffer
		int newSize = reqiredSize*2;
		//System.err.println("grow to "+newSize);
		byte[] newBuffer = new byte[newSize];
		System.arraycopy(buffer, position, newBuffer, 0, populated);	
		buffer = newBuffer;
	}
	
	public final int readBytesPosition(int length) {
		//ensure all the bytes are in the buffer before calling visitor
		if (position>limit - length) {
			fetch(length);
		}
		int result = position;
		position+=length;
		return result;
	}
	
	@Deprecated //use a mutable transfer object instead if possible
	public final byte[] getBuffer() {
		return buffer;
	}
	
	public final void readByteData(byte[] target, int offset, int length) {
		//ensure all the bytes are in the buffer before calling visitor
		if (position>limit - length) {
			fetch(length);
		}
		System.arraycopy(buffer, position, target, offset, length);
		position+=length;
	}
	
	//By writing pmap into byteconsumer we have a vitual lookup for the next byte
	//if it can be kept here then it can be inlined instead however groups
	//will often nest which will require a stack of pmaps.
	
	//*** this is the best idea! go with it. Flaw found with each of the others.
	
	//its not "REALLY" a stack its just a pre-empt of new bits to be read before
	//the rest of the list is finished so if all new are kept in an array as we
	//work our way down we can just add more on at that point then when we get
	//back to the old location it will naturally flow!
	
	//the zeros and dynamic length may pose a problem.
	//also stopping at non by boundaries for next group will pose a problem.
	//leave stop bits in bytes?
	
	/////
	//leave stop bit so we know when to stop and return zeros
	//group will call popPmap() at end of fields
	//must write pmap bytes onto list backwards.
	//* we know MAX of bits when reading but it may be shorter.
	//* read ahead like strings, buffer is holding max bytes.
	//each pmap needs a bit position byte on another stack. one per pmap only
	//OR each pmap byte is decoded in a method that pushes 7 valeus on or 8 if stop!!
	
	//TODO: duplicate this for the writer logic accumulating bits to be written on a single list
	//TODO: write unit tests around these functions.
	
	
	private final int INIT_PMAP_SIZE = 1024;
	
	byte[] pmapStack = new byte[INIT_PMAP_SIZE];
	byte[] pmapIdxStack = new byte[INIT_PMAP_SIZE>>2];
	
	int pmapStackDepth = 0;
	int pmapIdxStackDepth = 0;
	int pmapIdx = -1;
	
	//called at the start of each group unless group knows it has no pmap
	public final void readPMap(int pmapMaxSize) {
		//force internal buffer to grow if its not big enough for this pmap
		if (limit - position <= pmapMaxSize) {
			fetch(pmapMaxSize); //largest fetch
		}
		//there are no zero length pmaps these are determined by the parent pmap
		int start = position;
		byte[] b = buffer;
		int p = position;
		
		byte v = b[p++];
		while ((v&0x80)==0) {
			v = b[p++];
		}
		position = p;
		
		//ensure stack can hold p-start
		int needed = p-start;
		if (pmapStackDepth+needed > pmapStack.length) {
			//must copy and grow stack
			pmapStack = growBytes(pmapStack, pmapStackDepth+needed);
		}
		if (pmapIdxStackDepth == pmapIdxStack.length) {
			//must copy and grow stack
			pmapIdxStack = growBytes(pmapIdxStack, pmapIdxStackDepth);
		}
				
		//walk back wards across these and push them on the stack
		//the first bits to read will the the last thing put on the array
		int j = position;
		while (--j>=start) {			
			pmapStack[pmapStackDepth++] = b[j];
		}
		//push the old index for resume
		if (pmapIdx>0) {
			pmapIdxStack[pmapIdxStackDepth++]=(byte) pmapIdx;
		}
		//set next bit to read
		pmapIdx = 7;
		
	}
	
	private byte[] growBytes(byte[] old, int need) {
		int newSize = need*2;
		byte[] newBytes = new byte[newSize];
		System.arraycopy(old, 0, newBytes, 0, old.length);
		return newBytes;
	}

	//called at every field to determine operation
	public final int popPMapBit() {
		if (pmapIdx<0) {
			//must return all the trailing zeros for the bit map after hit end of map. see (a1)
			return 0;
		}
		byte block = pmapStack[pmapStackDepth-1];
		//get next bit and decrement the bit index pmapIdx
		int value = 1&(block>>>(--pmapIdx));
		if (pmapIdx==0) {
			pmapIdx=7;
			//if we have not reached the end of the map dec to the next byte
			if ((block&0x80)==0) {
				pmapStackDepth--;
			} else {
				//(a1) hit end of map, set this to < 0 so we return zeros until this pmap is popped off.
				pmapIdx=-1;
			}
		}
		return value;
	}	
	
	//called at the end of each group
	public final void popPMap() {
		assert((pmapStack[pmapStackDepth-1]&0x80)!=0) : "stack error in pmap processing";
		if (pmapIdxStackDepth>0) {
			pmapIdx = pmapIdxStack[--pmapIdxStackDepth];
			pmapStackDepth--;
		}
	}
	
	/////////////////////////////////////
	/////////////////////////////////////
	/////////////////////////////////////
	
	

	
	
	//find the stop bit for the ascii string to be used by CharSeqShadow
	public final void readASCII(CharSequenceShadow shadow) {
		//read until stop bit is encountered.
		//may need to shift fetch and even grow buffer to ensure its all in one block.

		if (limit - position < 2) {
			fetch(2);
		}
		
		//can read maxLength with no worry
		byte v = buffer[position];
		
		if (0 == v) {
			v = buffer[position+1];
			if (0x80 != (v&0xFF)) {
				throw new UnsupportedOperationException();
			}
			shadow.setBacking(buffer, position, 0);
			position+=2;
		} else {	
			//must use count because the base of position will be in motion.
			//however the position can not be incremented or fetch may drop data.
			int count = 0;
			
			while ((buffer[position+count]&0x80)==0) {
				count++;
				if (position+count>=limit) {
					fetch(1); //CAUTION: may change value of position
				}
			}
			count++;
			shadow.setBacking(buffer, position, count);
			position+=count;			
		}
		
	}
	
	
	
	
	//only moves the position forward if a null was found
	public final boolean peekNull() {
		if (position>=limit) {
			fetch(1);
		}
		return (0x80 == (buffer[position]&0xFF));	
	}
	
	public final void incPosition() {
		position++;
	}
	
	public final long readSignedLongNullable() {
		//TODO:rewrite
		long temp = readSignedLong();
		if (temp>0) {
			return temp-1;
		}
		return temp;
	}
	
	public final long readSignedLong () {
		if (limit-position<=10) {
			if (position>=limit) {
				fetch(1);
			}
			int v = buffer[position++];
			long accumulator = ((v&0x40)==0) ? 0 :0xFFFFFFFFFFFFFF80l;

		    while ((v & 0x80)==0) {
		    	if (position>=limit) {
					fetch(1);
				}
		    	accumulator = (accumulator|v)<<7;
		    	v = buffer[position++];
		    }
		    return accumulator|(v&0x7F);
		}
		
		int p = position;
		byte[] buff = this.buffer;
		
		
		byte v = buff[p++];
		long accumulator = ((v&0x40)==0) ? 0 :0xFFFFFFFFFFFFFF80l;

	    while ((v & 0x80)==0) {
	    	accumulator = (accumulator|v)<<7;
	    	v = buff[p++];
	    }
	    position = p;
	    return accumulator|(v&0x7F);
	}
	
	public final long readUnsignedLongNullable() {
		return readUnsignedLong()-1;
	}
	
	public final long readUnsignedLong () {
		if (position>limit-10) {
			if (position>=limit) {
				fetch(1);
			}
			byte v = buffer[position++];
			long accumulator;
			if ((v & 0x80)==0) {
				accumulator = v<<7;
			} else {
				return (v&0x7F);
			}
			
			if (position>=limit) {
				fetch(1);
			}
			v = buffer[position++];
			
		    while ((v & 0x80)==0) {
		    	accumulator = (accumulator|v)<<7;
		    	
		    	if (position>=limit) {
					fetch(1);
				}
		    	v = buffer[position++];
		    	
		    }
		    return accumulator|(v&0x7F);
		}
		byte[] buf = buffer;

		byte v = buf[position++];
		long accumulator;
		if ((v & 0x80)==0) {
			accumulator = v<<7;
		} else {
			return (v&0x7F);
		}
		
		v = buf[position++];
	    while ((v & 0x80)==0) {
	    	accumulator = (accumulator|v)<<7;
	    	v = buf[position++];
	    }
	    return accumulator|(v&0x7F);
	}
	
	public final int readSignedIntegerNullable() {
		//TODO:rewrite
		int temp = readSignedInteger();
		if (temp>0) {
			return temp-1;
		}
		return temp;
	}
	
	public final int readSignedInteger () {
		if (limit-position<=10) {
			if (position>=limit) {
				fetch(1);
			}
			int v = buffer[position++];
			int accumulator = ((v&0x40)==0) ? 0 :0xFFFFFF80;

		    while ((v & 0x80)==0) {
		    	if (position>=limit) {
					fetch(1);
				}
		    	accumulator = (accumulator|v)<<7;
		    	v = buffer[position++];
		    }
		    return accumulator|(v&0x7F);
		}
		
		int p = position;
		byte[] buff = this.buffer;
		
		
		byte v = buff[p++];
		int accumulator = ((v&0x40)==0) ? 0 :0xFFFFFF80;

	    while ((v & 0x80)==0) {
	    	accumulator = (accumulator|v)<<7;
	    	v = buff[p++];
	    }
	    position = p;
	    return accumulator|(v&0x7F);
	}
	
	public final int readUnsignedIntegerNullable() {
		return readUnsignedInteger()-1;
	}
	
	public final int readUnsignedInteger() {
		if (position>limit-10) {
			if (position>=limit) {
				fetch(1);
			}
			byte v = buffer[position++];
			int accumulator;
			if ((v & 0x80)==0) {
				accumulator = v<<7;
			} else {
				return (v&0x7F);
			}
			
			if (position>=limit) {
				fetch(1);
			}
			v = buffer[position++];

		    while ((v & 0x80)==0) {
		    	accumulator = (accumulator|v)<<7;
		    	if (position>=limit) {
					fetch(1);
				}
		    	v = buffer[position++];
		    }
		    return accumulator|(v&0x7F);
		}
		byte v = buffer[position++];
		int accumulator;
		if ((v & 0x80)==0) {
			accumulator = v<<7;
		} else {
			return (v&0x7F);
		}
		
		v = buffer[position++];
	    while ((v & 0x80)==0) {
	    	accumulator = (accumulator|v)<<7;
	    	v = buffer[position++];
	    }
	    return accumulator|(v&0x7F);
	}

	
}
