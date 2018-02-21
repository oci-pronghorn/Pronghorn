package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface ToNullableDecimalFunction<T> {
    //@FunctionalInterface
    interface Visit {
        void visit(double value, int precision, boolean isNull);
    }
    void applyAsDecimal(T o, Visit func);
}

