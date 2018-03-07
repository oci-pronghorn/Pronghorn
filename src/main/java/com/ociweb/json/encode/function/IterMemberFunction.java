package com.ociweb.json.encode.function;

// When used as the iterator, N==M, return null to stop, previous N passed in
//@FunctionalInterface
public interface IterMemberFunction<T, N, M> {
    M get(T obj, int i, N node);
}
