package com.ociweb.jfast.stream;

import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.FieldReaderText;
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
public final class FASTRingBuffer implements CharSequence {

	final int[] buffer;
	final int mask;
	
	final int maxSize;
	
	final int maxCharSize;
	final int charMask;
	final char[] charBuffer;
	public final TextHeap textHeap; 
	final char[] rawConstHeap;
	
	int[] tat;
	int[] initTat;
	char[] data;
	
	final AtomicInteger removeCount = new AtomicInteger();
	final AtomicInteger addCount = new AtomicInteger();
	int addCharPos = 0;
	private int addPos = 0;
	int remPos = 0;
		
	public FASTRingBuffer(byte bits, byte charBits, TextHeap heap) {
		assert(bits>=1);
		this.textHeap = heap;
		
		tat = null==textHeap?null:textHeap.tat;
		initTat = null==textHeap?null:textHeap.initTat;
		data = null==textHeap?null:textHeap.data;
		
		this.rawConstHeap = null==textHeap?null:textHeap.rawInitAccess();
		
		this.maxSize = 1<<bits;
		this.mask = maxSize-1;
		this.buffer = new int[maxSize];
			
		this.maxCharSize = 1<<charBits;
		this.charMask = maxCharSize-1;
		this.charBuffer = new char[maxCharSize];
	}
	
	/**
	 * Empty and restore to original values.
	 */
	public void reset() {
		addCharPos = 0;
		addPos = 0;
		remPos = 0;
		removeCount.set(0);
		addCount.set(0);		
	}
	
	//adjust these from the offset of the biginning of the message.
	
	public void release() {
		//step forward and allow write to previous location.
	}
	
	public void lockMessage() {
		//step forward to next message or sequence?
		//provide offset to the beginning of this message.
		
	}

	
	//TODO: B, model after java8 lambda behavior.  xxx.stream().filter(x).map(y).max() If objects can not escape or are reused this should be performant 
	//filter() use predicate to took at template id or sequence and while roll forward to next or pass on current.
	//         by avoid of write to ring buffer in first place majority of filters would give performance boost.
	//map() 
	//reduce()?  map.collect()
	
	/* Java8 code converting iterator into stream so TODO: B, add toIterator method
	 * <T> Stream<T> stream(Iterator<T> iterator) {
    Spliterator<T> spliterator
        = Spliterators.spliteratorUnknownSize(iterator, 0);
    return StreamSupport.stream(spliterator, false);
}
	 */
	
//TODO: C, Add Forgetful functor to ignore  messages OR sequence groups OR groups that do not need to be added to ring buffer.
//TODO: C, Add blocking consumer on the complete answer of preamble. (not sure this is first class)
	
	//TODO: B, add functors in general to do custom work before ring buffer
	//TODO: B, add functors in general to do custom work from ring buffer before encoding?
	
    public final int availableCapacity() {
    	return maxSize-(addPos-remPos);
    }
    
	
	public void appendBytes(byte[] source) {
		
		int i = 0;
		while (i<source.length) {
			buffer[mask&addPos++] = source[i++];
		}
	}

	
	//Text RingBuffer encoding for two ints
	// pos  pos  char ring buffer, length and position
	// neg  pos  heap constant index
	// pos  neg  null
	
	public final int appendInt1(int value) {
		buffer[mask&addPos++]=value;
		return value;
	}
	public final void appendInt2(int a, int b) {
		buffer[mask&addPos++]=a;
		buffer[mask&addPos++]=b;
	}
	public final void appendInt3(int a, int b, int c) {
		int M = mask;
		int p = addPos;
		buffer[M&p++]=a;
		buffer[M&p++]=b;
		buffer[M&p++]=c;
		addPos = p;
	}
	public final void appendInt4(int a, int b, int c, int d) {
		
		int M = mask;
		int p = addPos;
		buffer[M&p++]=a;
		buffer[M&p++]=b;
		buffer[M&p++]=c;
		buffer[M&p++]=d;
		addPos = p;
		
	}
	public final void appendInt5(int a, int b, int c, int d, int e) {
		int M = mask;
		int p = addPos;
		buffer[M&p++]=a;
		buffer[M&p++]=b;
		buffer[M&p++]=c;
		buffer[M&p++]=d;
		buffer[M&p++]=e;
		addPos = p;
	}
	
	public final void appendInt6(int a, int b, int c, int d, int e, int f) {
		int M = mask;
		int p = addPos;
		buffer[M&p++]=a;
		buffer[M&p++]=b;
		buffer[M&p++]=c;
		buffer[M&p++]=d;
		buffer[M&p++]=e;
		buffer[M&p++]=f;		
		addPos = p;
	}
	
	public final void appendInt7(int a, int b, int c, int d, int e, int f, int g) {
		int M = mask;
		int p = addPos;
		buffer[M&p++]=a;
		buffer[M&p++]=b;
		buffer[M&p++]=c;
		buffer[M&p++]=d;
		buffer[M&p++]=e;
		buffer[M&p++]=f;		
		buffer[M&p++]=g;	
		addPos = p;
	}
	
	public final void appendInt8(int a, int b, int c, int d, int e, int f, int g, int h) {
		int M = mask;
		int p = addPos;
		buffer[M&p++]=a;
		buffer[M&p++]=b;
		buffer[M&p++]=c;
		buffer[M&p++]=d;
		buffer[M&p++]=e;
		buffer[M&p++]=f;		
		buffer[M&p++]=g;	
		buffer[M&p++]=h;
		addPos = p;
	}

	public int writeTextToRingBuffer(int heapId, int len) {
		int p = addCharPos;
		if (len>0) {
			addCharPos+=len;
			//end with function call for performance.
			TextHeap.get(heapId, charBuffer, p, charMask,tat,data);
		}
		return p;
	}

	//TODO: B, Use static method to access fields by offset based on templateId.

	public void appendBytes(int idx, ByteHeap heap) {
		
		throw new UnsupportedOperationException();//TODO: C,copy text solution once finished.
		
	}

	//only called once the end of a group is reached and we want to allow the consumer to have access to the fields.
	public void moveForward() {
		//consumer is allowed to read up to addCount
		addCount.lazySet(addPos); 
	}
	
	public void removeForward(int step) {
		remPos = removeCount.get()+step;
		assert(remPos<=addPos);
		removeCount.lazySet(remPos);
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
	
		
	public int readCharsLength(int idx) {
		//second int is always the length 
		return buffer[mask&(remPos+idx+1)];
	}

	
	public void readBytes(int idx, byte[] target) {
		//TODO: X, Would it be better to store 4 bytes per int. this is not a compact form, check this later if we have performnce problems.
		int i = 0;
		while (i<target.length) {
			target[i] = (byte)buffer[mask&(remPos+idx+i)];
			i++;
		}
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
		//System.err.println("Pulling length value:"+buffer[mask&(remPos+charSeqIdx+1)]);
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

	//TODO: C, putting things in the ring buffer out of order may break the inner char ring buffer, MUST fix.
	
	@Override
	public CharSequence subSequence(int start, int end) {
		throw new UnsupportedOperationException();
	}

	public boolean hasContent() {
		return addPos>remPos;
	}

	public int contentRemaining() {
		return addPos-remPos;
	}

	
}
