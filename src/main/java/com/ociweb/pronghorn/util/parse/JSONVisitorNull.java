package com.ociweb.pronghorn.util;

public class JSONVisitorNull implements JSONVisitor {

	StringBuilder builder = new StringBuilder();
	
	@Override
	public Appendable stringValue() {
		builder.setLength(0);
		return builder;
	}

	@Override
	public void stringValueComplete() {
	}

	@Override
	public Appendable stringName(int fieldIndex) {
		builder.setLength(0);
		return builder;
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
