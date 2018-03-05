package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface IterStringFunction<T, N> {
    //@FunctionalInterface
    interface Visit {
        void visit(String v);
    }
    void applyAsString(T o, int i, N node, Visit visit);
}
