package com.ociweb.json.encode.function;

@FunctionalInterface
public interface IterLongFunction<T, N> {
    @FunctionalInterface
    interface Visit {
        void visit(long v);
    }
    void applyAsLong(T o, int i, N node, Visit visit);
}
