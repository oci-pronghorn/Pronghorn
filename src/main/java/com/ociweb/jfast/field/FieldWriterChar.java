package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FieldWriterChar {

	private final TextHeap heap;
	private final PrimitiveWriter writer;
	private final int INSTANCE_MASK;
	
	public FieldWriterChar(PrimitiveWriter writer, TextHeap charDictionary) {
		assert(charDictionary.itemCount()<TokenBuilder.MAX_INSTANCE);
		assert(FieldReaderInteger.isPowerOfTwo(charDictionary.itemCount()));
		
		this.INSTANCE_MASK = (charDictionary.itemCount()-1);
		this.heap = charDictionary;
		this.writer = writer;
	}

	public void writeUTF8CopyOptional(int token, CharSequence value) {
		
		int idx = token & INSTANCE_MASK;
	//	System.err.println("BB");
//		if (null == value) {
//			if (heap.isNull(idx)) {
//				writer.writePMapBit((byte)0);
//			} else {
//				writer.writePMapBit((byte)1);
//				writer.writeNull();
//				heap.setNull(idx);
//			}
//		} else {
			if (heap.equals(idx, value)) {
				writer.writePMapBit((byte)0);
			} else {
				writer.writePMapBit((byte)1);
				writer.writeIntegerUnsigned(value.length()+1);
				writer.writeTextUTF(value);
				heap.set(idx, value, 0, value.length());
			}
	//	}
	}

	public void writeUTF8DefaultOptional(int token, CharSequence value) {
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
				writer.writeIntegerUnsigned(value.length()+1);
				writer.writeTextUTF(value);
			}
		}
	}

	public void writeUTF8DeltaOptional(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		if (null==value) {
			writer.writeNull();
			heap.setNull(idx);
		} else {
					
			//count matching front or back chars
			int headCount = heap.countHeadMatch(idx, value);
			int tailCount = heap.countTailMatch(idx, value);
			if (headCount>tailCount) {
				writeUTF8Tail(value, idx, headCount, 1);
			} else {
				writeUTF8Head(value, idx, tailCount);
			}
		}
	}

	public void writeUTF8TailOptional(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		if (null==value) {
			writer.writeNull();
			heap.setNull(idx);
		} else {
			writeUTF8Tail(value, idx, heap.countHeadMatch(idx, value), 1);
		}
	}

	private void writeUTF8Tail(CharSequence value, int idx, int headCount, final int optional) {
		int trimTail = heap.length(idx)-headCount;
		writer.writeIntegerSigned(trimTail);
		
		int valueSend = value.length()-headCount;
		
		//System.err.println("write: trim:"+trimTail+" length:"+valueSend+" headCount:"+headCount);
		
		writer.writeIntegerUnsigned(valueSend+optional);//plus 1 for optional		
		writer.writeTextUTFAfter(headCount,value);
		heap.appendTail(idx, trimTail, headCount, value);
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
		
		if (heap.equals(idx, value)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(value.length());
			writer.writeTextUTF(value);
		}
	}

	
	public void writeUTF8Delta(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value);
		int tailCount = heap.countTailMatch(idx, value);
		if (headCount>tailCount) {
			writeUTF8Tail(value, idx, headCount, 0);
		} else {
			writeUTF8Head(value, idx, tailCount);
		}
	}

	private void writeUTF8Head(CharSequence value, int idx, int tailCount) {
		//replace head, tail matches to tailCount
		int trimHead = heap.length(idx)-tailCount;
		writer.writeIntegerUnsigned(-trimHead -1); //negative -1 for head append
		
		int valueSend = value.length()-tailCount;
		writer.writeIntegerUnsigned(valueSend); 		
		writer.writeTextUTFBefore(value, trimHead);
		heap.appendHead(idx, trimHead, value);
	}

	public void writeUTF8Tail(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		writeUTF8Tail(value, idx, heap.countHeadMatch(idx, value), 0);
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
		
		if (null==value) {
			writer.writeNull();
			heap.setNull(idx);
		} else {
			//count matching front or back chars
			int headCount = heap.countHeadMatch(idx, value);
			int tailCount = heap.countTailMatch(idx, value);
			if (headCount>tailCount) {
				writeASCIITail(idx, headCount, value);
			} else {
				writeASCIIHead(value, idx, tailCount);			
			}
		}
	}

	private void writeASCIITail(int idx, int headCount, CharSequence value) {

		int trimTail = heap.length(idx)-headCount;
		writer.writeIntegerUnsigned(trimTail);
		
		writer.writeTextASCIIAfter(headCount,value);
		heap.appendTail(idx, trimTail, headCount, value);
		
	}
	

	private void writeASCIIHead(CharSequence value, int idx, int tailCount) {
		
		int trimHead = heap.length(idx)-tailCount;
		writer.writeIntegerSigned(-trimHead );
		
		writer.writeTextASCIIBefore(value,trimHead);
		heap.appendHead(idx, trimHead, value);
	}
	
	public void writeASCIITailOptional(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		if (null==value) {
			writer.writeNull();
			heap.setNull(idx);
		} else {
			int headCount = heap.countHeadMatch(idx, value);		
			writeASCIITail(idx, headCount, value);
		}
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
		
		if (heap.equals(idx, value)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeTextASCII(value);
		}
	}
	
	public void writeASCIIDefaultOptional(int token, CharSequence value) {
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
			}
		}
	}


	public void writeUTF8DefaultOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length+1);
			writer.writeTextUTF(value,offset,length);
		}
	}
	
	public void writeASCIIDelta(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value);
		int tailCount = heap.countTailMatch(idx, value);
		if (headCount>tailCount) {
			writeASCIITail(idx, headCount, value);
		} else {
			writeASCIIHead(value, idx, tailCount);						
		}
	}


	public void writeASCIITail(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		writeASCIITail(idx, heap.countHeadMatch(idx, value), value);
	}

	public void writeUTF8DeltaOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, length, offset+length);
		if (headCount>tailCount) {
			writeUTF8Tail(idx, headCount, value, offset, length, 1);
		} else {
			writeUTF8Head(idx, tailCount, value, offset, length);
		}
	}

	public void writeUTF8TailOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		writeUTF8Tail(idx, headCount, value, offset, length, 1);
	}
	
	private void writeUTF8Tail(int idx, int headCount, char[] value, int offset, int length, final int optional) {
		int trimTail = heap.length(idx)-headCount;
		writer.writeIntegerUnsigned(trimTail);
		
		int valueSend = length-headCount;
		
		writer.writeIntegerUnsigned(valueSend+optional);
		writer.writeTextUTF(value, offset+headCount, valueSend);
		heap.appendTail(idx, trimTail, value, offset+headCount, valueSend);
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
	
	public void writeUTF8Constant(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length);
			writer.writeTextUTF(value,offset,length);
		}
	}

	public void writeUTF8Default(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length);
			writer.writeTextUTF(value,offset,length);
		}
	}
	


	public void writeUTF8Delta(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, length, offset+length);
		if (headCount>tailCount) {
			writeUTF8Tail(idx, headCount, value, offset+headCount, length-headCount, 0);
		} else {
			writeUTF8Head(idx, tailCount, value, offset, length);
		}
	}

	private void writeUTF8Head(int idx, int tailCount, char[] value, int offset, int length) {
		//replace head, tail matches to tailCount
		int trimHead = heap.length(idx)-tailCount;
		writer.writeIntegerSigned(-trimHead); //negative -1 for head append
		
		int len = length - tailCount;
		writer.writeIntegerUnsigned(len);
		writer.writeTextUTF(value, offset, len);
		
		heap.appendHead(idx, trimHead, value, offset, len);
	}

	public void writeUTF8Tail(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		writeUTF8Tail(idx, heap.countHeadMatch(idx, value, offset, length), value, offset, length, 0);
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
		int tailCount = heap.countTailMatch(idx, value, length, offset+length);
		if (headCount>tailCount) {
			writeASCIITail(idx, headCount, value, offset, length);
			
		} else {
			//replace head, tail matches to tailCount
			int trimHead = heap.length(idx)-tailCount;
			writer.writeIntegerSigned(-trimHead -1); //negative -1 for head append
			
			int len = length - tailCount;
			writer.writeTextASCII(value, offset, len);
			
			heap.appendHead(idx, trimHead, value, offset, len);
		}
	}

	public void writeASCIITailOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		writeASCIITail(idx, heap.countHeadMatch(idx, value, offset, length), value, offset, length);
	}
		
	private void writeASCIITail(int idx, int headCount, char[] value, int offset, int length) {
		int trimTail = heap.length(idx)-headCount; //head count is total that match from head.
		writer.writeIntegerSigned(trimTail); //cut off these from tail
		
		int valueSend = length-headCount;
		int valueStart = offset+headCount;
			
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

	public void writeASCIIConstant(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeTextASCII(value, offset, length);
		}
	}

	public void writeASCIIDefault(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeTextASCII(value, offset, length);
		}
	}

	public void writeASCIIDefaultOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeTextASCII(value, offset, length);
		}
	}
	
	public void writeASCIIDelta(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, length, offset+length);
		if (headCount>tailCount) {
			writeASCIITail(idx, headCount, value, offset, length);
			
		} else {
			//replace head, tail matches to tailCount
			int trimHead = heap.length(idx)-tailCount;
			writer.writeIntegerUnsigned(-trimHead -1); //negative -1 for head append
			
			int len = length - tailCount;
			writer.writeTextASCII(value, offset, len);
			
			heap.appendHead(idx, trimHead, value, offset, len);
		}
	}

	public void writeASCIITail(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		writeASCIITail(idx, heap.countHeadMatch(idx, value, offset, length), value, offset, length);
	}

	public void writeNull(int token) {
		
		if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
			if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
				//None and Delta (both do not use pmap)
		//		writeClearNull(token);              //no pmap, yes change to last value
			} else {
				//Copy and Increment
		//		writePMapAndClearNull(token);  //yes pmap, yes change to last value	
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
		//		writePMapNull(token);  //yes pmap,  no change to last value
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
	
//	private void writeClearNull(int token) {
//		writer.writeNull();
//		lastValue[token & INSTANCE_MASK] = 0;
//	}
//	
//	
//	private void writePMapAndClearNull(int token) {
//		int idx = token & INSTANCE_MASK;
//
//		if (lastValue[idx]==0) { //stored value was null;
//			writer.writePMapBit((byte)0);
//		} else {
//			writer.writePMapBit((byte)1);
//			writer.writeNull();
//			lastValue[idx] =0;
//		}
//	}
//	
//	
//	private void writePMapNull(int token) {
//		if (lastValue[token & INSTANCE_MASK]==0) { //stored value was null;
//			writer.writePMapBit((byte)0);
//		} else {
//			writer.writePMapBit((byte)1);
//			writer.writeNull();
//		}
//	}
	

}
