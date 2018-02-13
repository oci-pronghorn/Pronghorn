package com.ociweb.json.encode.function;

@FunctionalInterface
public interface IterLongFunction<T> {
    @FunctionalInterface
    interface Visit {
        void visit(long v);
    }
    void applyAsLong(T o, int i, Visit visit);
}
