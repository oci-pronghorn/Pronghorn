package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface IteratorFunction<T, N> {
    N get(T obj, int i, N node);
}
