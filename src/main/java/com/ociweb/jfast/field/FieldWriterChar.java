package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FieldWriterChar {

	private final TextHeap heap;
	private final PrimitiveWriter writer;
	private final int INSTANCE_MASK;
	
	public FieldWriterChar(PrimitiveWriter writer, TextHeap charDictionary) {
		assert(charDictionary.textCount()<TokenBuilder.MAX_INSTANCE);
		assert(FieldReaderInteger.isPowerOfTwo(charDictionary.textCount()));
		
		this.INSTANCE_MASK = (charDictionary.textCount()-1);
		this.heap = charDictionary;
		this.writer = writer;
	}

	public void writeUTF8CopyOptional(int token, CharSequence value) {
		
		int idx = token & INSTANCE_MASK;
		
		if (null == value) {
			if (heap.isNull(idx)) {
				writer.writePMapBit((byte)0);
			} else {
				writer.writePMapBit((byte)1);
				writer.writeNull();
				heap.setNull(idx);
			}
		} else {
			if (heap.equals(idx, value)) {
				writer.writePMapBit((byte)0);
			} else {
				writer.writePMapBit((byte)1);
				writer.writeIntegerUnsigned(value.length()+1);
				writer.writeTextUTF(value);
				heap.set(idx, value, 0, value.length());
			}
		}
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
				//replace head, tail matches to tailCount
				int trimHead = heap.length(idx)-tailCount;
				writer.writeIntegerUnsigned(-trimHead -1); //negative -1 for head append
				
				int valueSend = value.length()-tailCount;
				writer.writeIntegerUnsigned(valueSend); 		
				writer.writeTextUTFBefore(value, trimHead);
				heap.appendHead(idx, trimHead, value);
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
		writer.writeIntegerUnsigned(trimTail);
		
		int valueSend = value.length()-headCount;
		writer.writeIntegerUnsigned(valueSend+optional);//plus 1 for optional		
		writer.writeTextUTFAfter(headCount,value);
		heap.appendTail(idx, trimTail, headCount, value);
	}

	public void writeUTF8Copy(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(value.length());
			writer.writeTextUTF(value);
			heap.set(idx, value, 0, value.length());
		}
	}

	public void writeUTF8Constant(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(value.length());
			writer.writeTextUTF(value);
		}
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
			//replace head, tail matches to tailCount
			int trimHead = heap.length(idx)-tailCount;
			writer.writeIntegerUnsigned(-trimHead -1); //negative -1 for head append
			
			int valueSend = value.length()-tailCount;
			writer.writeIntegerUnsigned(valueSend); 		
			writer.writeTextUTFBefore(value, trimHead);
			heap.appendHead(idx, trimHead, value);
		}
	}

	public void writeUTF8Tail(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		int headCount = heap.countHeadMatch(idx, value);	
		writeUTF8Tail(value, idx, headCount, 0);
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
				//replace head, tail matches to tailCount
				int trimHead = heap.length(idx)-tailCount;
				writer.writeIntegerUnsigned(-trimHead -1); //negative -1 for head append
				
				writer.writeTextASCIIBefore(value,trimHead);
				heap.appendHead(idx, trimHead, value);			
			}
		}
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

	private void writeASCIITail(int idx, int headCount, CharSequence value) {
		int trimTail = heap.length(idx)-headCount;
		writer.writeIntegerUnsigned(trimTail);
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

	public void writeASCIIConstant(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeTextASCII(value);
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

	public void writeASCIIDelta(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value);
		int tailCount = heap.countTailMatch(idx, value);
		if (headCount>tailCount) {
			writeASCIITail(idx, headCount, value);
		} else {
			//replace head, tail matches to tailCount
			int trimHead = heap.length(idx)-tailCount;
			writer.writeIntegerUnsigned(-trimHead -1); //negative -1 for head append
			writer.writeTextASCIIBefore(value,trimHead);
			heap.appendHead(idx, trimHead, value);						
		}
	}

	public void writeASCIITail(int token, CharSequence value) {
		int idx = token & INSTANCE_MASK;
		int headCount = heap.countHeadMatch(idx, value);
		writeASCIITail(idx, headCount, value);
	}

	public void writeUTF8CopyOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
				
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length+1);
			writer.writeTextUTF(value,offset,length);
			heap.set(idx, value, offset, length);
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

	public void writeUTF8DeltaOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, offset, length);
		if (headCount>tailCount) {
			writeUTF8Tail(idx, headCount, value, offset, length, 1);
		} else {
			//replace head, tail matches to tailCount
			int trimHead = heap.length(idx)-tailCount;
			writer.writeIntegerUnsigned(-trimHead -1); //negative -1 for head append
			
			int valueSend = length-tailCount;
			writer.writeIntegerUnsigned(valueSend); 
			writer.writeTextUTF(value, offset, valueSend);
			heap.appendHead(idx, trimHead, value, offset, valueSend);
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
		} else {
			writer.writePMapBit((byte)1);
			writer.writeIntegerUnsigned(length);
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
		int tailCount = heap.countTailMatch(idx, value, offset, length);
		if (headCount>tailCount) {
			writeUTF8Tail(idx, headCount, value, offset, length,0);
			
		} else {
			//replace head, tail matches to tailCount
			int trimHead = heap.length(idx)-tailCount;
			writer.writeIntegerUnsigned(-trimHead -1); //negative -1 for head append
			
			int len = length - tailCount;
			writer.writeIntegerUnsigned(len);
			writer.writeTextUTF(value, offset, len);
			
			heap.appendHead(idx, trimHead, value, offset, len);
		}
	}

	public void writeUTF8Tail(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		writeUTF8Tail(idx, headCount, value, offset, length, 0);
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

	public void writeASCIIDefaultOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value, offset, length)) {
			writer.writePMapBit((byte)0);
		} else {
			writer.writePMapBit((byte)1);
			writer.writeTextASCII(value, offset, length);
		}
	}

	public void writeASCIIDeltaOptional(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, offset, length);
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
		int trimTail = heap.length(idx)-headCount;
		writer.writeIntegerSigned(trimTail);
		
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

	public void writeASCIIDelta(int token, char[] value, int offset, int length) {
		int idx = token & INSTANCE_MASK;
		
		//count matching front or back chars
		int headCount = heap.countHeadMatch(idx, value, offset, length);
		int tailCount = heap.countTailMatch(idx, value, offset, length);
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
