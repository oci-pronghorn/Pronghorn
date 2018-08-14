package com.ociweb.pronghorn.util.parse;

import java.io.IOException;

import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ByteConsumer;

public class JSONStreamVisitorCapture<A extends Appendable> implements JSONStreamVisitor {

	final A target;
	int depth = 0;
	boolean objectStart = true;
    boolean simpleArray = false;
	
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
	
	public JSONStreamVisitorCapture(A target) {
		this.target = target;
	}
	
	
	@Override
	public void nameSeparator() {
		try {
			objectStart = false;
			target.append(':');
			simpleArray = false;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void endObject() {
		try {
			
			depth--;
			target.append('}').append('\n');
			objectStart = true;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void beginObject() {
		try {
			
			depth++;
			target.append('{').append('\n');
			objectStart=true;
			simpleArray = false;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void beginArray() {
		try {
			target.append('[');
			simpleArray = true;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void endArray() {
		try {
			target.append(']');
			simpleArray = false;
			objectStart = true;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void valueSeparator() {
		try {
			target.append(',');
			if (!simpleArray) {
				target.append('\n');
			}
			objectStart = true;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void whiteSpace(byte b) {
		
	}

	@Override
	public void literalTrue() {
		try {
			target.append("true");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void literalNull() {
		try {
			target.append("null");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void literalFalse() {
		try {
			target.append("false");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void numberValue(long m, byte e) {
		Appendables.appendDecimalValue(target, m, e);
	}

	@Override
	public void stringBegin() {
		try {
			if (objectStart) {
				for(int i = 0;i<depth;i++) {target.append("    ");}
			}
			target.append("\"");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public ByteConsumer stringAccumulator() {
		builder.setLength(0);
		return con;
	}

	@Override
	public void stringEnd() {
		try {
			target.append(builder);
			target.append("\"");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}


	@Override
	public void customString(int id) {
		throw new UnsupportedOperationException();
	}


	@Override
	public boolean isReady() {
		return true;
	}

}
