package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface IterDoubleFunction<T, N> {
    //@FunctionalInterface
    interface Visit {
        void visit(double v);
    }
    void applyAsDouble(T o, int i, N node, Visit visit);
}
