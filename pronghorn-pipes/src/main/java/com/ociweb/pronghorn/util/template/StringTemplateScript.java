package com.ociweb.pronghorn.util.template;

import com.ociweb.pronghorn.util.AppendableByteWriter;

//@FunctionalInterface
public abstract class StringTemplateScript<T> {
	public abstract void render(AppendableByteWriter appendable, T source);
}
