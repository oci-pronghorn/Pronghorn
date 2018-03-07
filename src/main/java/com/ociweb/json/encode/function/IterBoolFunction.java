package com.ociweb.json.encode.function;

//@FunctionalInterface
public interface IterBoolFunction<T, N> {
    //@FunctionalInterface
    interface Visit {
        void visit(boolean v);
    }
    void applyAsBool(T o, int i, N node, Visit visit);
}

