package com.ociweb.json.encode.function;

//@FunctionalInterface
@Deprecated
public interface IterNullableLongFunction<T> {
    //@FunctionalInterface
    interface Visit {
        void visit(long v, boolean isNull);
    }
    void applyAsLong(T o, int i, Visit visit);
}
