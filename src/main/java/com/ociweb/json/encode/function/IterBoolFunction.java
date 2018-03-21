package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface IterBoolFunction<T, N> {
    boolean applyAsBool(T o, int i, N node);
}

