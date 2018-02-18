package com.ociweb.json.encode.function;

@FunctionalInterface
public interface ArrayIteratorFunction<T, N> {
    // Return null to stop.
    // What is returned is passed into next iteration.
    N test(T o, int i, N node);
}
