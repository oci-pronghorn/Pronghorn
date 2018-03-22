package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface IterMemberFunction<T, M> {
    M get(T obj, int i);
}

