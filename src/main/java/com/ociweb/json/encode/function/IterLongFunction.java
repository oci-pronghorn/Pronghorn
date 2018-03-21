package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface IterLongFunction<T, N> {
    long applyAsLong(T o, int i, N node);
}
