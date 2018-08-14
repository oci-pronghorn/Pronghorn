package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface ToDoubleFunction<T> {
    double applyAsDouble(T value);
}
