package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface ToEnumFunction<T, E extends Enum<E>> {
    E applyAsEnum(T value);
}

