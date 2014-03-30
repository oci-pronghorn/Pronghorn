package com.ociweb.jfast.stream;

public interface DispatchObserver {

	void tokenItem(long absPos, int token, int cursor, String value);

}
