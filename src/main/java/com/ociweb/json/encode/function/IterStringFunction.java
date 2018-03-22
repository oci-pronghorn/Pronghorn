package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface IterStringFunction<T> {
    CharSequence applyAsString(T o, int i);
}
