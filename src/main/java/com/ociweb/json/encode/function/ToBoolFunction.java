package com.ociweb.json.encode.function;

@FunctionalInterface
public interface ToBoolFunction<T> {
    boolean applyAsBool(T value);
}
