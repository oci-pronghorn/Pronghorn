package com.ociweb.pronghorn.util.template;

import com.ociweb.pronghorn.util.AppendableByteWriter;

//@FunctionalInterface
public interface StringTemplateScript<T> {
	void render(AppendableByteWriter appendable, T source);
}
