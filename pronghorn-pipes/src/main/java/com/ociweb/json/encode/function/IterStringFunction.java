package com.ociweb.json.encode.function;

import com.ociweb.pronghorn.util.AppendableByteWriter;

//@FunctionalInterface
public interface IterStringFunction<T> {
    void applyAsString(T o, int i, AppendableByteWriter<?> target);
}
