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
public final class FASTRingBuffer  {

	final int[] buffer;
	final int mask;
	
	final int maxSize;
	
	final int maxCharSize;
	final int charMask;
	final char[] charBuffer;
	public final TextHeap textHeap; 
	final char[] rawConstHeap;
	
	FASTFilter filter = FASTFilter.none;
	
	
	final AtomicInteger removeCount = new AtomicInteger();
	final AtomicInteger addCount = new AtomicInteger();
	int addCharPos = 0;
	int addPos = 0;
	int remPos = 0;
		
	public FASTRingBuffer(byte primaryBits, byte charBits, TextHeap heap) {
		assert(primaryBits>=1);
		this.textHeap = heap;
		
	
		this.rawConstHeap = null==textHeap?null:textHeap.rawInitAccess();
		
		this.maxSize = 1<<primaryBits;
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
	
	
	//TODO: A, add map method which can take data from one ring buffer and populate another.

	//TODO: A, Promises/Futures/Listeners as possible better fit to stream processing?
	//TODO: A, look at adding reduce method in addition to filter.

    public final int availableCapacity() {
    	return maxSize-(addPos-remPos);
    }
    
    public static int appendi(int[] buf, int pos, int mask, int value) {
        buf[mask&pos]=value;
        return pos+1;
    }
	
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
	
	//TODO: Z, add map toIterator method for consuming ring buffer by java8 streams.

	public int writeTextToRingBuffer(int heapId, int len) {
		final int p = addCharPos;
		if (len>0) {		
			addCharPos = textHeap.copyToRingBuffer(heapId, charBuffer, p, charMask);
		}
		return p;
	}

	//TODO: A, Use static method to access fields by offset based on templateId.
	//TODO: A, At end of group check filter of that record and jump back if need to skip.

	public void appendBytes(int idx, ByteHeap heap) {
		
		throw new UnsupportedOperationException();//TODO: C,copy text solution once finished.
		
	}
	
	//next sequence is ready for consumption.
	public void unBlockSequence() {
		//TODO: A, only filter on the message level. sequence will be slow and difficult because they are nested.
		
		//if filtered out the addPos will be rolled back to newGroupPos
		byte f = filter.go(addCount.get(),this);//TODO: what if we need to split first sequence and message header.
		if (f>0) {//consumer is allowed to read up to addCount
			//normal 
			addCount.lazySet(addPos); 			
		} else if (f<0) {
			//skip
			addPos=addCount.get();
		} //else hold
	}
	
	//
	public void unBlockMessage() {
		//if filtered out the addPos will be rolled back to newGroupPos
		byte f = filter.go(addCount.get(),this);//TODO: B, may call at end of message, can not change mind!
		if (0==f) {
			//do not hold use default instead
			f = filter.defaultBehavior();
		}
		
		if (f>0) {//consumer is allowed to read up to addCount
			//normal 
			addCount.lazySet(addPos); 			
		} else {
			//skip
			addPos=addCount.get();
		}
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
	
	//TODO: A, Given templateId, and FieldId return offset for RingBuffer to get value, must keep in client code
	
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
		int ref1 = buffer[mask&(remPos+fieldPos)];
		//constant from heap or dynamic from char ringBuffer
		return ref1<0 ? textHeap.initStartOffset(ref1) : ref1;
	}

	public char[] readRingCharBuffer(int fieldPos) {
		//constant from heap or dynamic from char ringBuffer
		return buffer[mask&(remPos+fieldPos)]<0 ? this.rawConstHeap : this.charBuffer;	
	}
	
	public int readRingCharMask() {
		return charMask;
	}


	public boolean hasContent() {
		return addPos>remPos;
	}

	public int contentRemaining() {
		return addPos-remPos;
	}

	
}
