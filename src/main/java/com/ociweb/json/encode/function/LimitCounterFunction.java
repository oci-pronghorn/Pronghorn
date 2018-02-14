package com.ociweb.json.encode.function;

@FunctionalInterface
public interface LimitCounterFunction<T, N> {
    N test(T o, int i, N node);
}
