package com.ociweb.json.encode.function;

@FunctionalInterface
public interface ToNullableLongFunction<T> {
    @FunctionalInterface
    interface Visit {
        void visit(long v, boolean isNull);
    }
    void applyAsLong(T o, Visit func);
}

