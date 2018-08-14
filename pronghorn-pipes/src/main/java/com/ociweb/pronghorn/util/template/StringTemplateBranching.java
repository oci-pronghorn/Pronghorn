package com.ociweb.pronghorn.util.template;

//@FunctionalInterface
public interface StringTemplateBranching<T> {
    int branch(T source);
}
