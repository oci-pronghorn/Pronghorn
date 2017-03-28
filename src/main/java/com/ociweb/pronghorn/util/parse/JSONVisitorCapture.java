package com.ociweb.pronghorn.util;

public class JSONVisitorCapture implements JSONVisitor {

	StringBuilder builder = new StringBuilder();
	
	@Override
	public Appendable stringValue() {
		return builder;
	}

	@Override
	public void stringValueComplete() {
		System.err.println(builder);
	}

	@Override
	public Appendable stringName(int fieldIndex) {
		return builder;
	}

	@Override
	public void stringNameComplete() {
		System.err.println(builder);
	}

	@Override
	public void arrayBegin() {
		System.err.println('[');
	}

	@Override
	public void arrayEnd() {
		System.err.println(']');
	}

	@Override
	public void arrayIndexBegin(int instance) {
		System.err.println("idx: "+instance);
	}

	@Override
	public void numberValue(long m, byte e) {
		System.err.println("A number");
		
	}

	@Override
	public void nullValue() {
		System.err.println("null");
	}

	@Override
	public void booleanValue(boolean b) {
		System.err.println("boolean "+b);
	}

	@Override
	public void objectEnd() {
		System.err.println("}");
	}

	@Override
	public void objectBegin() {
		System.err.println("}");
	}

}
