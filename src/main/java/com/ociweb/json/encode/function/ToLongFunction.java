package com.ociweb.json.encode.function;

@FunctionalInterface
public interface ToLongFunction<T> {
    long applyAsLong(T value);
}