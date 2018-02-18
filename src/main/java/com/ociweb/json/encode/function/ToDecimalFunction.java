package com.ociweb.json.encode.function;

@FunctionalInterface
public interface ToDecimalFunction<T> {
    @FunctionalInterface
    interface Visit {
        void visit(long m, byte e);
    }
    void applyAsDecimal(T o, Visit func);
}
