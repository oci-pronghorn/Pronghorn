//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import java.nio.ByteBuffer;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FieldWriterBytes {

	private final ByteHeap heap;
	private final PrimitiveWriter writer;
	private final int INSTANCE_MASK;
	
	public FieldWriterBytes(PrimitiveWriter writer, ByteHeap byteDictionary) {
		assert(byteDictionary.itemCount()<TokenBuilder.MAX_INSTANCE);
		assert(FieldReaderInteger.isPowerOfTwo(byteDictionary.itemCount()));
		
		this.INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (byteDictionary.itemCount()-1));
		
		this.heap = byteDictionary;
		this.writer = writer;
	}
	
	public void reset(DictionaryFactory df) {
		df.reset(heap);
	}	
	public void copy(int sourceToken, int targetToken) {
		//replace string at target with string found in source.
		heap.copy(sourceToken & INSTANCE_MASK, targetToken & INSTANCE_MASK);
	}

	private void writeClearNull(int token) {
		writer.writeNull();
		heap.setNull(token & INSTANCE_MASK);
	}
	
	private void writePMapNull(int token) {
		if (heap.isNull(token & INSTANCE_MASK)) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeNull();
		}
	}
	
	private void writePMapAndClearNull(int token) {
		int idx = token & INSTANCE_MASK;

		if (heap.isNull(idx)) { //stored value was null;
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeNull();
			heap.setNull(idx);
		}
	}
	
	public void writeNull(int token) {
		
		if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
			if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
				//None and Delta and Tail
				writeClearNull(token);              //no pmap, yes change to last value
			} else {
				//Copy and Increment
				writePMapAndClearNull(token);  //yes pmap, yes change to last value	
			}
		} else {
			if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
				if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
					//const
					writer.writeNull();                 //no pmap,  no change to last value  
				} else {
					//const optional
					writer.writePMapBit((byte)0);       //pmap only
				}			
			} else {	
				//default
				writePMapNull(token);  //yes pmap,  no change to last value
			}	
		}
		
	}

	static int count = 0;
	
	public void writeBytesTail(int token, ByteBuffer value) {
		int idx = token & INSTANCE_MASK;
				
		writeBytesTail(idx, heap.countHeadMatch(idx, value), value, 0);
		value.position(value.limit());//skip over the data just like we wrote it.
		
		if (++count<10) {
			//System.err.println("1 TailBytesWritten:"+(writer.totalWritten()-start));
		}
		
	}

	public void writeBytesTailOptional(int token, ByteBuffer value) {
		int idx = token & INSTANCE_MASK;
		writeBytesTail(idx, heap.countHeadMatch(idx, value), value, 1);
		value.position(value.limit());//skip over the data just like we wrote it.
	}

	public void writeBytesDelta(int token, ByteBuffer value) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value);
		int tailCount = heap.countTailMatch(idx, value);
		if (headCount>tailCount) {
			writeBytesTail(idx, headCount, value, 0); //does not modify position
		} else {
			writeBytesHead(idx, tailCount, value, 0); //does not modify position
		}
		value.position(value.limit());//skip over the data just like we wrote it.
	}

	public void writeBytesDeltaOptional(int token, ByteBuffer value) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value);
		int tailCount = heap.countTailMatch(idx, value);
		if (headCount>tailCount) {
			writeBytesTail(idx, headCount, value, 1); //does not modify position
		} else {
			writeBytesHead(idx, tailCount, value, 1); //does not modify position
		}
		value.position(value.limit());//skip over the data just like we wrote it.
	}

	private void writeBytesHead(int idx, int tailCount, ByteBuffer value, int opt) {
		
		//replace head, tail matches to tailCount
		int trimHead = heap.length(idx)-tailCount;
		writer.writeIntegerSigned(trimHead==0? opt: -trimHead); 
		
		int len = value.remaining() - tailCount;
		int offset = value.position();
		writer.writeIntegerUnsigned(len);
		writer.writeByteArrayData(value, offset, len);
		heap.appendHead(idx, trimHead, value, offset, len);
	}
	
	
	private void writeBytesTail(int idx, int headCount, ByteBuffer value, final int optional) {
		
		int trimTail = heap.length(idx)-headCount;
		if (trimTail<0) {
			throw new ArrayIndexOutOfBoundsException();
		}
		writer.writeIntegerUnsigned(trimTail>=0? trimTail+optional : trimTail);
		
		int valueSend = value.remaining()-headCount;
		int startAfter = value.position()+headCount;
				
		writer.writeIntegerUnsigned(valueSend);
		//System.err.println("tail send:"+valueSend+" for headCount "+headCount);
		heap.appendTail(idx, trimTail, value, startAfter, valueSend);
		writer.writeByteArrayData(value, startAfter, valueSend);
		
	}
	/*
	 * 	int trimTail = heap.length(idx)-headCount;
		writer.writeIntegerUnsigned(trimTail);
		
		int valueSend = length-headCount;
		int startAfter = offset+headCount;
		int sendLen = valueSend+optional;
		
		writer.writeIntegerUnsigned(sendLen);
		writer.writeByteArrayData(value, startAfter, valueSend);
		heap.appendTail(idx, trimTail, value, startAfter, valueSend);
	 */
	

	public void writeBytesCopy(int token, ByteBuffer value) {
		int idx = token & INSTANCE_MASK;
		//System.err.println("AA");
		if (heap.equals(idx, value)) {
			writer.writePMapBit((byte)0);
			value.position(value.limit());//skip over the data just like we wrote it.
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(value.remaining());
			heap.set(idx, value);//position is NOT modified
			writer.writeByteArrayData(value); //this moves the position in value
		}
	}

	public void writeBytesDefault(int token, ByteBuffer value) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value)) {
			writer.writePMapBit((byte)0);
			value.position(value.limit());//skip over the data just like we wrote it.
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(value.remaining());
			writer.writeByteArrayData(value); //this moves the position in value
		}
	}

	public void writeBytes(ByteBuffer value) {
		writer.writeIntegerUnsigned(value.remaining());
		writer.writeByteArrayData(value); //this moves the position in value
	}

	public void writeBytesOptional(ByteBuffer value) {
		writer.writeIntegerUnsigned(value.remaining()+1);
		writer.writeByteArrayData(value); //this moves the position in value
	}
	
	public void writeBytesConstant(int token) {
		//nothing need be sent because constant does not use pmap and the template
		//on the other receiver side will inject this value from the template
	}
	
	public void writeBytesCopyOptional(int token, ByteBuffer value) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value)) {
			writer.writePMapBit((byte)0);
			value.position(value.limit());//skip over the data just like we wrote it.
		} 
		else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(value.remaining()+1);
			heap.set(idx, value);//position is NOT modified
			writer.writeByteArrayData(value); //this moves the position in value
		}
	}

	public void writeBytesDefaultOptional(int token, ByteBuffer value) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value)) {
			writer.writePMapBit((byte)0); 
			value.position(value.limit());//skip over the data just like we wrote it.
		} else {
			writer.writePMapBit((byte)1);
			int len = value.remaining();
			if (len<0) {
				len = 0;
			}
			writer.writeIntegerUnsigned(len);
			writer.writeByteArrayData(value);
		}
	}


	public void writeBytesConstantOptional(int token) {
		writer.writePMapBit((byte)1);
		//the writeNull will take care of the rest.
	}
	
	public void writeBytesOptional(byte[] value, int offset, int length) {
		writer.writeIntegerUnsigned(length+1);
		writer.writeByteArrayData(value,offset,length);
	}

	public void writeBytesTailOptional(int token, byte[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		writeBytesTail(idx, headCount, value, offset, length, 1);
	}
	
	private void writeBytesTail(int idx, int headCount, byte[] value, int offset, int length, final int optional) {
		int trimTail = heap.length(idx)-headCount;
		writer.writeIntegerUnsigned(trimTail>=0? trimTail+optional: trimTail);
		
		int valueSend = length-headCount;
		int startAfter = offset+headCount;
		
		writer.writeIntegerUnsigned(valueSend);
		writer.writeByteArrayData(value, startAfter, valueSend);
		heap.appendTail(idx, trimTail, value, startAfter, valueSend);
	}

	public void writeBytesDeltaOptional(int token, byte[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, offset+length, length);
		if (headCount>tailCount) {
			writeBytesTail(idx, headCount, value, offset, length, 1);
		} else {
			writeBytesHead(idx, tailCount, value, offset, length, 1);
		}
	}

	private void writeBytesHead(int idx, int tailCount, byte[] value, int offset, int length, int opt) {
		
		//replace head, tail matches to tailCount
		int trimHead = heap.length(idx)-tailCount;
		writer.writeIntegerSigned(trimHead==0? opt: -trimHead); 
		
		int len = length - tailCount;
		writer.writeIntegerUnsigned(len);
		writer.writeByteArrayData(value, offset, len);
		
		heap.appendHead(idx, trimHead, value, offset, len);
	}
	
	public void writeBytesCopyOptional(int token, byte[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length+1);
			writer.writeByteArrayData(value,offset,length);
			heap.set(idx, value, offset, length);
		}
	}

	public void writeBytesDefaultOptional(int token, byte[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length);
			writer.writeByteArrayData(value,offset,length);
		}
	}

	public void writeBytes(byte[] value, int offset, int length) {
		writer.writeIntegerUnsigned(length);
		writer.writeByteArrayData(value,offset,length);
	}

	public void writeBytesTail(int token, byte[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		writeBytesTail(idx, heap.countHeadMatch(idx, value, offset, length), value, offset, length, 0);
	
		if (++count<10) {
			//writer.flush();
			//System.err.println("2 TailBytesWritten:"+(writer.totalWritten()-start));
		}
	}

	public void writeBytesDelta(int token, byte[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, offset+length, length);
		if (headCount>tailCount) {
			writeBytesTail(idx, headCount, value, offset+headCount, length, 0);
		} else {
			writeBytesHead(idx, tailCount, value, offset, length, 0);
		}
	}
	
	public void writeBytesCopy(int token, byte[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		}
		else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length);
			writer.writeByteArrayData(value,offset,length);
			heap.set(idx, value, offset, length);
		}
	}

	public void writeBytesDefault(int token, byte[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length);
			writer.writeByteArrayData(value,offset,length);
		}
	}
}
