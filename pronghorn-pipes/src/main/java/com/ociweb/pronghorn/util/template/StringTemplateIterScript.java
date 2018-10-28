package com.ociweb.pronghorn.util.template;

import com.ociweb.pronghorn.util.AppendableByteWriter;

//@FunctionalInterface
public interface StringTemplateIterScript<T> {
	boolean render(AppendableByteWriter<?> appendable, T source, int i);
}

