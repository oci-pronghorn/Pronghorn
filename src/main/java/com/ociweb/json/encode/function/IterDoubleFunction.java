package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface IterDoubleFunction<T, N> {
    double applyAsDouble(T o, int i, N node);
}
