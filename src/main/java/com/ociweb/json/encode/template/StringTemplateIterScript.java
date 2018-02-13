package com.ociweb.json.encode.template;

import com.ociweb.json.encode.appendable.AppendableByteWriter;

public interface StringTemplateIterScript<T> {
	boolean fetch(AppendableByteWriter apendable, T source, int i);
}

