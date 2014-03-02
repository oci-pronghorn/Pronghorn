//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FieldWriterChar {

	private final TextHeap heap;
	private final PrimitiveWriter writer;
	private final int INSTANCE_MASK;
	private static final int INIT_VALUE_MASK = 0x80000000;
	
	public FieldWriterChar(PrimitiveWriter writer, TextHeap charDictionary) {
		assert(null==charDictionary || charDictionary.itemCount()<TokenBuilder.MAX_INSTANCE);
		assert(null==charDictionary || FieldReaderInteger.isPowerOfTwo(charDictionary.itemCount()));
		
		this.INSTANCE_MASK = null==charDictionary? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (charDictionary.itemCount()-1));
		this.heap = charDictionary;
		this.writer = writer;
	}

	public void reset(DictionaryFactory df) {
		df.reset(heap);
	}	
	public void copy(int sourceToken, int targetToken) {
		//replace string at target with string found in source.
		heap.copy(sourceToken & INSTANCE_MASK, targetToken & INSTANCE_MASK);
	}
	
	public void writeUTF8CopyOptional(int token, CharSequence value) {
		
		int idx = token & INSTANCE_MASK;

		if (heap.equals(idx, value)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(value.length()+1);
			writer.writeTextUTF(value);
			heap.set(idx, value, 0, value.length());
		}
	}

	public void writeUTF8DefaultOptional(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		if (null==value) {
			if (heap.isNull(idx|INIT_VALUE_MASK)) {
				writer.writePMapBit((byte)0);
			} else {
				writer.writePMapBit((byte)1);
				writer.writeNull();
			}
		} else {
			if (heap.equals(idx|INIT_VALUE_MASK, value)) {
				writer.writePMapBit((byte)0);
			} else {
				writer.writePMapBit((byte)1);
				writer.writeIntegerUnsigned(value.length()+1);
				writer.writeTextUTF(value);
			}
		}
	}

	public void writeUTF8DeltaOptional(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;

		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value);
		int tailCount = heap.countTailMatch(idx, value);
		if (headCount>tailCount) {
			int trimTail = heap.length(idx)-headCount;
			writer.writeIntegerSigned(trimTail>=0? trimTail+1 : trimTail); //plus 1 for optional
			int length = (value.length()-headCount);		
			writeUTF8Tail(idx, trimTail, headCount, value, length);
		} else {
			writeUTF8Head(value, idx, tailCount, 1);
		}

	}
	
	public void writeUTF8Delta(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value);
		int tailCount = heap.countTailMatch(idx, value);
		if (headCount>tailCount) {
			int trimTail = heap.length(idx)-headCount;
			writer.writeIntegerSigned(trimTail);
			writeUTF8Tail(idx, trimTail, headCount, value, value.length()-headCount);
		} else {
			writeUTF8Head(value, idx, tailCount, 0);
		}
	}

	
	private void writeUTF8Head(CharSequence value, int idx, int tailCount, int optional) {
		
		//replace head, tail matches to tailCount
		int trimHead = heap.length(idx)-tailCount;
		writer.writeIntegerSigned(0==trimHead? optional : -trimHead); 

		int valueSend = value.length()-tailCount;
		
		writer.writeIntegerUnsigned(valueSend); 		
		writer.writeTextUTFBefore(value, valueSend);
		heap.appendHead(idx, trimHead, value, valueSend);
	}
	
	public void writeUTF8TailOptional(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		int headCount = heap.countHeadMatch(idx, value);
		int trimTail = heap.length(idx)-headCount;
		writer.writeIntegerUnsigned(trimTail+1);//plus 1 for optional
		int length = (value.length()-headCount);		
		writeUTF8Tail(idx, trimTail, headCount, value, length);
	}
	
	public void writeUTF8Tail(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		int headCount = heap.countHeadMatch(idx, value);
		int trimTail = heap.length(idx)-headCount;
		writer.writeIntegerUnsigned(trimTail);
		int length = (value.length()-headCount);
		writeUTF8Tail(idx, trimTail, headCount, value, length);
	}

	private void writeUTF8Tail(int idx, int firstRemove, int startAfter, CharSequence value, int length) {
		writer.writeIntegerUnsigned(length);
		writer.writeTextUTFAfter(startAfter,value);
		heap.appendTail(idx, firstRemove, startAfter, value);
	}

	public void writeUTF8Copy(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		//System.err.println("AA");
		if (heap.equals(idx, value)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(value.length());
			writer.writeTextUTF(value);
			heap.set(idx, value, 0, value.length());
		}
	}
	

	public void writeASCIIConstant(int token) {
		//nothing need be sent because constant does not use pmap and the template
		//on the other receiver side will inject this value from the template
	}
	
	public void writeASCIIConstantOptional(int token) {
		writer.writePMapBit((byte)1);
		//the writeNull will take care of the rest.
	}
	
	
	public void writeUTF8Constant(int token) {
		//nothing need be sent because constant does not use pmap and the template
		//on the other receiver side will inject this value from the template
	}
	
	public void writeUTF8ConstantOptional(int token) {
		writer.writePMapBit((byte)1);
	}
	
	
	

	public void writeUTF8Default(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx|INIT_VALUE_MASK, value)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(value.length());
			writer.writeTextUTF(value);
		}
	}



	public void writeASCIICopyOptional(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		if (null==value) {
			if (heap.isNull(idx)) {
				writer.writePMapBit((byte)0);
			} else {
				writer.writePMapBit((byte)1);
				writer.writeNull();
			}
		} else {
			if (heap.equals(idx, value)) {
				writer.writePMapBit((byte)0);
			} else {
				writer.writePMapBit((byte)1);
				writer.writeTextASCII(value);
				heap.set(idx, value, 0, value.length());
			}
		}
	}

	

	public void writeASCIIDeltaOptional(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value);
		int tailCount = heap.countTailMatch(idx, value);
		if (headCount>tailCount) {
			int trimTail = heap.length(idx)-headCount;
			writer.writeIntegerSigned(trimTail); 
			writeASCIITail(idx, headCount, value, trimTail);
		} else {
			writeASCIIHead(idx, tailCount, value);			
		}
	}

	public void writeASCIIDelta(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value);
		int tailCount = heap.countTailMatch(idx, value);
		if (headCount>tailCount) {
			int trimTail = heap.length(idx)-headCount;
			if (trimTail<0) {
				throw new UnsupportedOperationException(trimTail+"");
			}
			writer.writeIntegerSigned(trimTail);
			writeASCIITail(idx, headCount, value, trimTail);
		} else {
			writeASCIIHead(idx, tailCount, value);						
		}
	}
	
	private void writeASCIIHead(int idx, int tailCount, CharSequence value) {
		
		int trimHead = heap.length(idx)-tailCount;
		writer.writeIntegerSigned(-trimHead );
		
		int sentLen = value.length()-tailCount;
		writer.writeTextASCIIBefore(value, sentLen);
		heap.appendHead(idx, trimHead, value, sentLen);
	}
	
	public void writeASCIITailOptional(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		int headCount = heap.countHeadMatch(idx, value);
		int trimTail = heap.length(idx)-headCount;
		
		if (trimTail<0) {
			System.err.println("2 write tail? "+trimTail);
		}
		
		writer.writeIntegerUnsigned(trimTail+1); 
		writeASCIITail(idx, headCount, value, trimTail);

	}

	public void writeASCIITail(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		int headCount = heap.countHeadMatch(idx, value);
		int trimTail = heap.length(idx)-headCount;
		writer.writeIntegerUnsigned(trimTail); 
		writeASCIITail(idx, headCount, value, trimTail);
	}
	
	private void writeASCIITail(int idx, int headCount, CharSequence value, int trimTail) {
		writer.writeTextASCIIAfter(headCount,value);
		heap.appendTail(idx, trimTail, headCount, value);
	}
	

	




	public void writeASCIICopy(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeTextASCII(value);
			heap.set(idx, value, 0, value.length());
		}
	}


	public void writeASCIIDefault(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx|INIT_VALUE_MASK, value)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeTextASCII(value);
		}
	}
	
	public void writeASCIIDefaultOptional(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		if (null==value) {
			if (heap.isNull(idx|INIT_VALUE_MASK)) {
				writer.writePMapBit((byte)0);
			} else {
				writer.writePMapBit((byte)1);
				writer.writeNull();
			}
		} else {
			if (heap.equals(idx|INIT_VALUE_MASK, value)) {
				writer.writePMapBit((byte)0);
			} else {
				writer.writePMapBit((byte)1);
				writer.writeTextASCII(value);
			}
		}
	}


	public void writeUTF8DefaultOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx|INIT_VALUE_MASK, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length+1);
			writer.writeTextUTF(value,offset,length);
		}
	}
	

	public void writeUTF8DeltaOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, offset+length, length);
		if (headCount>tailCount) {
			writeUTF8Tail(idx, headCount, value, offset, length, 1);
		} else {
			writeUTF8Head(idx, tailCount, value, offset, length, 1);
		}
	}

	public void writeUTF8TailOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		writeUTF8Tail(idx, heap.countHeadMatch(idx, value, offset, length), value, offset, length, 1);
	}
	
	public void writeUTF8Delta(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, offset+length, length);
		if (headCount>tailCount) {
			writeUTF8Tail(idx, headCount, value, offset+headCount, length, 0);
		} else {
			writeUTF8Head(idx, tailCount, value, offset, length, 0);
		}
	}
	
	public void writeUTF8Tail(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		writeUTF8Tail(idx, heap.countHeadMatch(idx, value, offset, length), value, offset, length, 0);
	}
	
	private void writeUTF8Tail(int idx, int headCount, char[] value, int offset, int length, final int optional) {
		int trimTail = heap.length(idx)-headCount;
		writer.writeIntegerUnsigned(trimTail+optional);
		
		int valueSend = length-headCount;
		int startAfter = offset+headCount;
		writer.writeIntegerUnsigned(valueSend);
		writer.writeTextUTF(value, startAfter, valueSend);
		heap.appendTail(idx, trimTail, value, startAfter, valueSend);
	}

	public void writeUTF8Copy(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		}
		else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length);
			writer.writeTextUTF(value,offset,length);
			heap.set(idx, value, offset, length);
		}
	}

	public void writeUTF8CopyOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} 
		else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length+1);
			writer.writeTextUTF(value,offset,length);
			heap.set(idx, value, offset, length);
		}
	}


	public void writeUTF8Default(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx|INIT_VALUE_MASK, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length);
			writer.writeTextUTF(value,offset,length);
		}
	}
	

	private void writeUTF8Head(int idx, int tailCount, char[] value, int offset, int length, int opt) {
		
		//replace head, tail matches to tailCount
		int trimHead = heap.length(idx)-tailCount;
		writer.writeIntegerSigned(trimHead==0? opt : -trimHead); 
		
		int len = length - tailCount;
		writer.writeIntegerUnsigned(len);
		writer.writeTextUTF(value, offset, len);
		
		heap.appendHead(idx, trimHead, value, offset, len);
	}


	public void writeASCIICopyOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeTextASCII(value, offset, length);
			heap.set(idx, value, offset, length);
		}
	}



	public void writeASCIIDeltaOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, offset+length, length);
		if (headCount>tailCount) {
			int trimTail = heap.length(idx)-headCount; //head count is total that match from head.
			writer.writeIntegerSigned(trimTail); //cut off these from tail
			
			int valueSend = length-headCount;
			int valueStart = offset+headCount;
				
			writeASCIITail(idx, trimTail, value, valueStart, valueSend);
			
		} else {
			//replace head, tail matches to tailCount
			int trimHead = heap.length(idx)-tailCount;
			writer.writeIntegerSigned(-trimHead);
			
			int len = length - tailCount;
			writer.writeTextASCII(value, offset, len);
			
			heap.appendHead(idx, trimHead, value, offset, len);

		}
	}

	public void writeASCIITailOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int trimTail = heap.length(idx)-headCount; //head count is total that match from head.
		if (trimTail<0) {
			System.err.println("write tail? "+trimTail);
		}
		writer.writeIntegerUnsigned(trimTail+1); //cut off these from tail
		
		int valueSend = length-headCount;
		int valueStart = offset+headCount;
			
		writeASCIITail(idx, trimTail, value, valueStart, valueSend);
	}
	
	
	public void writeASCIIDelta(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, offset+length, length);
		if (headCount>tailCount) {
			int trimTail = heap.length(idx)-headCount; //head count is total that match from head.
			writer.writeIntegerSigned(trimTail); //cut off these from tail
			
			int valueSend = length-headCount;
			int valueStart = offset+headCount;
				
			writeASCIITail(idx, trimTail, value, valueStart, valueSend);
			
		} else {
			//replace head, tail matches to tailCount
			int trimHead = heap.length(idx)-tailCount;
			writer.writeIntegerUnsigned(-trimHead);
			
			int len = length - tailCount;
			writer.writeTextASCII(value, offset, len);
			
			heap.appendHead(idx, trimHead, value, offset, len);
		}
	}

	public void writeASCIITail(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int trimTail = heap.length(idx)-headCount; //head count is total that match from head.
		writer.writeIntegerUnsigned(trimTail); //cut off these from tail
		
		int valueSend = length-headCount;
		int valueStart = offset+headCount;
			
		writeASCIITail(idx, trimTail, value, valueStart, valueSend);
	}
		
	private void writeASCIITail(int idx, int trimTail, char[] value, int valueStart, int valueSend) {
		writer.writeTextASCII(value, valueStart, valueSend);
		heap.appendTail(idx, trimTail, value, valueStart, valueSend);
	}
	
	public void writeASCIICopy(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeTextASCII(value, offset, length);
			heap.set(idx, value, offset, length);
		}
	}


	public void writeASCIIDefault(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx|INIT_VALUE_MASK, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeTextASCII(value, offset, length);
		}
	}

	public void writeASCIIDefaultOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx|INIT_VALUE_MASK, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeTextASCII(value, offset, length);
		}
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

	public void writeUTF8Optional(char[] value, int offset, int length) {
		writer.writeIntegerUnsigned(length+1);
		writer.writeTextUTF(value,offset,length);
	}

	public void writeUTF8(char[] value, int offset, int length) {
		writer.writeIntegerUnsigned(length);
		writer.writeTextUTF(value,offset,length);
	}

	public void writeUTF8Optional(CharSequence value) {//If null??
		writer.writeIntegerUnsigned(value.length()+1);
		writer.writeTextUTF(value);
	}
	
	public void writeUTF8(CharSequence value) {
		writer.writeIntegerUnsigned(value.length());
		writer.writeTextUTF(value);
	}

	public void writeASCII(CharSequence value) {
		writer.writeTextASCII(value);
	}

	public void writeASCIITextOptional(int token, char[] value, int offset, int length) {
		writer.writeTextASCII(value,offset,length);
	}

	public void writeASCIITextOptional(int token, CharSequence value) {
		writer.writeTextASCII(value);
	}

	public void writeASCIIText(int token, char[] value, int offset, int length) {
		writer.writeTextASCII(value,offset,length);
	}

}
