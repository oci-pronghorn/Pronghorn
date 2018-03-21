package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface IterMemberFunction<T, N, M> {
    M get(T obj, int i, N node);
}

