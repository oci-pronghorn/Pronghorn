package com.ociweb.json.encode.function;

@FunctionalInterface
public interface ToNullableBoolFunction<T> {
    @FunctionalInterface
    interface Visit {
        void visit(boolean b, boolean isNull);
    }
    void applyAsBool(T o, Visit func);
}
