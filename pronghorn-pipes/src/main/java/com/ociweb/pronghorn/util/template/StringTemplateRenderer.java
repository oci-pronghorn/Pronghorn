package com.ociweb.pronghorn.util.template;

import com.ociweb.pronghorn.util.AppendableByteWriter;

public abstract class StringTemplateRenderer<T> {

	public abstract void render(AppendableByteWriter<?> writer, T source);
	
}
