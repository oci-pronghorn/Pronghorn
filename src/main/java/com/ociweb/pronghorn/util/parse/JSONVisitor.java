package com.ociweb.pronghorn.util;

public interface JSONVisitor<A extends Appendable>  {

	A stringValue();

	void stringValueComplete();

	A stringName(int fieldIdx);

	void stringNameComplete();

	void arrayBegin();

	void arrayEnd();

	void arrayIndexBegin(int instance);

	void numberValue(long m, byte e);

	void nullValue();

	void booleanValue(boolean b);

	void objectEnd();

	void objectBegin();

	
}
