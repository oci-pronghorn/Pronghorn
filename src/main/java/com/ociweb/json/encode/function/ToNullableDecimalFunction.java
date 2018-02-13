package com.ociweb.json.encode.function;

@FunctionalInterface
public interface ToNullableDecimalFunction<T> {
    @FunctionalInterface
    interface Visit {
        void visit(long m, byte e, boolean isNull);
    }
    void applyAsDecimal(T o, Visit func);
}

