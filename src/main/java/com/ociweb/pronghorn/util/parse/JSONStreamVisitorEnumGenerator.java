package com.ociweb.pronghorn.util.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ByteConsumer;

public class JSONStreamVisitorEnumGenerator implements JSONStreamVisitor {

	final StringBuilder builder = new StringBuilder();
	final Set<String> keys = new HashSet<String>();
	
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
	
	public JSONStreamVisitorEnumGenerator() {
	}
	
	
	@Override
	public void nameSeparator() {
		keys.add(builder.toString());
	}

	@Override
	public void endObject() {
		builder.setLength(0);
	}

	@Override
	public void beginObject() {
		
	}

	@Override
	public void beginArray() {
		
	}

	@Override
	public void endArray() {
		builder.setLength(0);
	}

	@Override
	public void valueSeparator() {
		builder.setLength(0);
	}

	@Override
	public void whiteSpace(byte b) {
		
	}

	@Override
	public void literalTrue() {
	}

	@Override
	public void literalNull() {
	}

	@Override
	public void literalFalse() {
	}

	@Override
	public void numberValue(long m, byte e) {
	}

	@Override
	public void stringBegin() {

	}

	@Override
	public ByteConsumer stringAccumulator() {
		builder.setLength(0);
		return con;
	}

	@Override
	public void stringEnd() {	
	}


	@Override
	public void customString(int id) {
		throw new UnsupportedOperationException();
	}

	
	public void generate(Appendable target) {
		
		
		ArrayList<String> list = new ArrayList<String>(keys);
		
		list.sort(new Comparator<String>(){

			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
			
		});
		
		
		for(String key: list) {
			
			try {
				target.append("    ").append(key.toUpperCase()).append("(\"").append(key).append("\"),\n");
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
						
		}
		
	}


	@Override
	public boolean isReady() {
		return true;
	}
	
}
