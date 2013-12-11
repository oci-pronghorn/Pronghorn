package com.ociweb.jfast.field;

import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FieldWriterChar {

	private final TextHeap heap;
	private final PrimitiveWriter writer;
	
	//crazy big value? TODO: make smaller mask based on exact length of array.
	private final int INSTANCE_MASK = 0xFFFFF;//20 BITS
	
	public FieldWriterChar(PrimitiveWriter writer, TextHeap charDictionary) {
		this.heap = charDictionary;
		this.writer = writer;
	}

	public void writeUTF8CopyOptional(int token, CharSequence value) {
		
		int idx = token & INSTANCE_MASK;
		
		if (heap.equals(idx, value)) {
			
		};
		// TODO Auto-generated method stub
		
		//heap.s
		
	}

	public void writeUTF8DefaultOptional(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8DeltaOptional(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
		//count matching front or back chars
		
	}

	public void writeUTF8TailOptional(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
		//count matching front chars
		
		
	}

	public void writeUTF8Copy(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8Constant(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8Default(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8Delta(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8Tail(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIICopyOptional(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIIDefaultOptional(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIIDeltaOptional(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIITailOptional(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIICopy(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIIConstant(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIIDefault(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIIDelta(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIITail(int token, CharSequence value) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8CopyOptional(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8DefaultOptional(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8DeltaOptional(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8TailOptional(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8Copy(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8Constant(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8Default(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8Delta(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeUTF8Tail(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIICopyOptional(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIIDefaultOptional(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIIDeltaOptional(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIITailOptional(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIICopy(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIIConstant(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIIDefault(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIIDelta(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

	public void writeASCIITail(int token, char[] value, int offset, int length) {
		// TODO Auto-generated method stub
		
	}

}
