package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface IterStringFunction<T, N> {
    CharSequence applyAsString(T o, int i, N node);
}
