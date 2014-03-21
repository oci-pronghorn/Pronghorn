package com.ociweb.jfast.stream;

import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.FieldReaderChar;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.TemplateCatalog;

/**
 * Specialized ring buffer for holding decoded values from a FAST stream.
 * Ring buffer has blocks written which correspond to whole messages or sequence items.
 * Within these blocks the consumer is provided random (eg. direct) access capabilities.
 * 
 * 
 * 
 * @author Nathan Tippy
 *
 */
public class FASTRingBuffer implements CharSequence {

	final AtomicInteger removeCount = new AtomicInteger();
	final int[] buffer;
	final int mask;
	final AtomicInteger addCount = new AtomicInteger();
	final int maxSize;
	
	final int maxCharSize;
	final int charMask;
	final char[] charBuffer;
	final TextHeap textHeap; 
	final char[] rawConstHeap;
	
	int addCharPos = 0;
	
	int addPos = 0;
	int remPos = 0;
		
	public FASTRingBuffer(byte bits, byte charBits, TextHeap heap) {
		assert(bits>=1);
		this.textHeap = heap;
		this.rawConstHeap = textHeap.rawInitAccess();
		
		this.maxSize = 1<<bits;
		this.mask = maxSize-1;
		this.buffer = new int[maxSize];
			
		this.maxCharSize = 1<<charBits;
		this.charMask = maxCharSize-1;
		this.charBuffer = new char[maxCharSize];
	}
	
	//adjust these from the offset of the biginning of the message.
	
	public void release() {
		//step forward and allow write to previous location.
	}
	
	public void lockMessage() {
		//step forward to next message or sequence?
		//provide offset to the beginning of this message.
		
	}
	
	//fields by type are fixed length however char is not so then what?
	//we know the type by the order however we do NOT have random access?
	//or we could add direct jump value table in the front.
	//or we could skip the fields we do not want decoded.
	//or we could read the buffer sequentially.
	
	public int getInt(int idx) {
		return buffer[idx];
	}
	
	public long getLong(int idx) {
		return (((long)buffer[idx])<<32)|(0xFFFFFFFFl&buffer[idx+1]);
	}
	
 
	
//    public boolean isBlocked(int need) {
//    	assert(need<maxSize) : "Ring buffer is not large enough to hold template";
//    	return (addPos-remPos)>=(maxSize-need); 
////           (addpos-rempos)+need>=maxSize;
//    	//ok
//    	//  (addPos-remPos)+need<maxSize
//    	//   need < maxSize - (addPos-remPos);
//    }
    public int availableCapacity() {
    	return maxSize-(addPos-remPos);
    }
    
	
	public final void append(int value) {
		buffer[mask&addPos++] = value;
	}
	
	public void append(long value) {		
		buffer[mask&addPos++] = (int)(value>>>32);
		buffer[mask&addPos++] = (int)(value&0xFFFFFFFF);
	}

	
	//Text RingBuffer encoding for two ints
	// pos  pos  char ring buffer, length and position
	// neg  pos  heap constant index
	// pos  neg  null
	
	public void appendText(int heapId) {
		
		if (heapId<0) {//points to constant in hash, high bit already set.
			buffer[mask&addPos++] = heapId; //must be neg - constants only
			buffer[mask&addPos++] = textHeap.length(heapId);//length.		
		} else {
			assert(heapId>=0) : "Only supported for primary values";
			if (textHeap.isNull(heapId)) {
				buffer[mask&addPos++] = 0;
				buffer[mask&addPos++] = -1;
			} else {
		    	storeTextInRingBuffer(heapId);
			}
		}						
	}

	private void storeTextInRingBuffer(int heapId) {
		//must store length in char sequence and store the position index.
		//with two ints can store both length and position.
		int len = textHeap.length(heapId);
		buffer[mask&addPos++] = addCharPos;//offset in text
		buffer[mask&addPos++] = len;//length of text

        //copy text into ring buffer.
		if (len>0) {
			textHeap.get(heapId, charBuffer, addCharPos, charMask);
			addCharPos+=len;
		}
	}

	public void append(int idx, ByteHeap heap) {
		
		throw new UnsupportedOperationException();//TODO: copy text solution once finished.
		
	}

	public void append(int readDecimalExponent, long readDecimalMantissa) {

		buffer[mask&addPos++] = readDecimalExponent;
		buffer[mask&addPos++] = (int)(readDecimalMantissa>>>32);
		buffer[mask&addPos++] = (int)(readDecimalMantissa&0xFFFFFFFF);
				
	}

	
	//only called once the end of a group is reached and we want to allow the consumer to have access to the fields.
	public void moveForward() {
		//consumer is allowed to read up to addCount
		addCount.lazySet(addPos); 
	}
	
	public void removeForward(int step) {
		//remPos = removeCount.get()+step;
		//removeCount.lazySet(remPos);
	}
	
	public void dump() {
		//move the removePosition up to the addPosition
		//System.err.println("resetup to "+addPos);
		remPos = addPos;
		removeCount.lazySet(addPos);
	}

	public void printPos(String label) {
		System.err.println(label+" remPos:"+remPos+"  addPos:"+addPos);
	}
	
	public int readInteger(int idx) {
		return buffer[mask&(remPos+idx)];
	}
	
	public long readLong(int idx) {
		
		int i = remPos+idx;
		return (((long)buffer[mask&i])<<32) | (((long)buffer[mask&(i+1)])&0xFFFFFFFFl);

	}
		
	public int getCharLength(int idx) {
		//second int is always the length 
		return buffer[mask&(remPos+idx+1)];
	}

	//this is for fast direct WRITE TO target
	public void readChars(int idx, char[] target, int targetIdx) {
		int ref1 = buffer[mask&(remPos+idx)];
		if (ref1<0) {
			textHeap.get(ref1, target, targetIdx);
		} else {
			int len = buffer[mask&(remPos+idx+1)];
			//copy into target but may need to loop from text buffer
			while (--len>=0) {
				target[targetIdx+len] = charBuffer[(ref1+len)&charMask];
			}
		}
	}
	
	//WARNING: consumer of these may need to loop around end of buffer !!
    //these are needed for fast direct READ FROM here
	public  int readRingCharPos(int fieldPos) {
		int ref1 = buffer[fieldPos];
		//constant from heap or dynamic from char ringBuffer
		return ref1<0 ? textHeap.initStartOffset(ref1) : ref1;
	}

	public char[] readRingCharBuffer(int fieldPos) {
		//constant from heap or dynamic from char ringBuffer
		return buffer[fieldPos]<0 ? this.rawConstHeap : this.charBuffer;	
	}
	
	public int readRingCharMask() {
		return charMask;
	}

	int charSeqIdx;
	
	public void selectCharSequence(int idx) {
		charSeqIdx = idx;
	}
	
	@Override
	public int length() {
		return buffer[mask&(remPos+charSeqIdx+1)];
	}

	@Override
	public char charAt(int at) {
		int ref1 = buffer[mask&(remPos+charSeqIdx)];
		if (ref1<0) {
			return textHeap.charAt(ref1,at);
		} else {
			return charBuffer[(ref1+at)&charMask];
		}
	}

	//TODO: putting things in the ring buffer out of order may break the inner char ring buffer, MUST fix.
	
	@Override
	public CharSequence subSequence(int start, int end) {
		throw new UnsupportedOperationException();
	}

	public boolean hasContent() {
		return addPos>remPos;
	}
	
}
