package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface IterMemberFunction<T, N, M> {
    M apply(T obj, int i, N node);
}
