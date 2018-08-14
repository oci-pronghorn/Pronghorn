package com.ociweb.json.encode.function;

import com.ociweb.pronghorn.util.AppendableByteWriter;

//@FunctionalInterface
public interface ToStringFunction<T> {
	void applyAsString(T value, AppendableByteWriter<?> target);
}
