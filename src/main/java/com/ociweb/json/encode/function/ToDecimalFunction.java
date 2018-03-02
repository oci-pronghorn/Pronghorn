package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface ToDecimalFunction<T> {
    //@FunctionalInterface
    interface Visit {
        void visit(double value, int precision);
    }
    void applyAsDecimal(T o, Visit func);
}
