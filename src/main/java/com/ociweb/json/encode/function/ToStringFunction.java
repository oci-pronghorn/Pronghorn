package com.ociweb.json.encode.function;

@FunctionalInterface
public interface ToStringFunction<T> {
    CharSequence applyAsString(T value);
}
