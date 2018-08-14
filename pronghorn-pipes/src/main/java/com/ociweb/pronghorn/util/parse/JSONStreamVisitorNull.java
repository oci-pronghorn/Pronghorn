package com.ociweb.pronghorn.util.parse;

import com.ociweb.pronghorn.util.ByteConsumer;

public class JSONStreamVisitorNull implements JSONStreamVisitor {

	protected ByteConsumer consumer = new ByteConsumer(){

		@Override
		public void consume(byte[] backing, int pos, int len, int mask) {	
		}

		@Override
		public void consume(byte value) {
		}
		
	};
	
	@Override
	public void nameSeparator() {
	}

	@Override
	public void endObject() {
	}

	@Override
	public void beginObject() {
	}

	@Override
	public void beginArray() {
	}

	@Override
	public void endArray() {
	}

	@Override
	public void valueSeparator() {
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
		return consumer;
	}

	@Override
	public void stringEnd() {
	}

	@Override
	public void customString(int id) {	
	}

	@Override
	public boolean isReady() {
		return true;
	}

}
