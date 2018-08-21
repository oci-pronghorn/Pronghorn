package com.ociweb.pronghorn.util;

public interface PHAppendable<T extends PHAppendable<T>> extends Appendable {
	

	T append(CharSequence csq);

	T append(CharSequence csq, int start, int end);

	T append(char c);
}
