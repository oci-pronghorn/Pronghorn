package com.ociweb.json.encode.function;

@FunctionalInterface
public interface ToNullableIntFunction<T> {
    @FunctionalInterface
    interface Visit {
        void visit(int v, boolean isNull);
    }
    void applyAsLong(T o, Visit func);
}
