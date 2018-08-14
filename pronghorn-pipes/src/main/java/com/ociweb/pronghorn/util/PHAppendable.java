package com.ociweb.pronghorn.util;

public interface PHAppendable<T extends PHAppendable<T>> extends Appendable {
	
	/**
	 * Drop everything written so far and return cursor to beginning of the stream
	 */
	void reset();
	
	T append(CharSequence csq);

	T append(CharSequence csq, int start, int end);

	T append(char c);
}
