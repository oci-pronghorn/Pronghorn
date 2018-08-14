package com.ociweb.pronghorn.util.parse;

import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ByteConsumer;

public class JSONVisitorNull implements JSONVisitor {

	final StringBuilder builder = new StringBuilder();
	
	final ByteConsumer con = new ByteConsumer() {

		@Override
		public void consume(byte[] backing, int pos, int len, int mask) {
			Appendables.appendUTF8(builder, backing, pos, len, mask);	
		}

		@Override
		public void consume(byte value) {
			builder.append((char)value);
		}				
	};
	
	@Override
	public ByteConsumer stringValue() {
		builder.setLength(0);
		return con;
	}

	@Override
	public void stringValueComplete() {
	}

	@Override
	public ByteConsumer stringName(int fieldIndex) {
		builder.setLength(0);
		return con;
	}

	@Override
	public void stringNameComplete() {
	}

	@Override
	public void arrayBegin() {
	}

	@Override
	public void arrayEnd() {
	}

	@Override
	public void arrayIndexBegin(int instance) {
	}

	@Override
	public void numberValue(long m, byte e) {		
	}

	@Override
	public void nullValue() {
	}

	@Override
	public void booleanValue(boolean b) {
	}

	@Override
	public void objectEnd() {
	}

	@Override
	public void objectBegin() {
	}

}
