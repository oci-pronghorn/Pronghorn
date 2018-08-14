package com.ociweb.pronghorn.util.parse;

import java.io.IOException;

import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ByteConsumer;

public class JSONVisitorCapture<A extends Appendable> implements JSONVisitor {

	final A target;
	
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
	
	public JSONVisitorCapture(A target) {
		this.target = target;
	}

	@Override
	public ByteConsumer stringValue() {
		builder.setLength(0);
		return con;
	}

	@Override
	public void stringValueComplete() {
		try {
			target.append(builder);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ByteConsumer stringName(int fieldIndex) {
		builder.setLength(0);
		
		if (fieldIndex>0) {
			builder.append(',');
		}
		
		return con;
	}

	@Override
	public void stringNameComplete() {
		try {
			target.append(builder).append(':');
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void arrayBegin() {
		try {
			target.append('[');
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void arrayEnd() {
		try {
			target.append(']');
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void arrayIndexBegin(int instance) {
		try {
			if (instance>0) {
				target.append(',');
			}
			
			//target.append("idx: ");
			//Appendables.appendValue(target, instance);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void numberValue(long m, byte e) {
		Appendables.appendDecimalValue(target, m, e);
	}

	@Override
	public void nullValue() {
		try {
			target.append("null");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void booleanValue(boolean b) {
		System.err.println("boolean "+b);
		
		try {
			target.append("boolean ");
			target.append(Boolean.toString(b));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public void objectEnd() {
		try {
			target.append('}');
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void objectBegin() {
		try {
			target.append('{');
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
