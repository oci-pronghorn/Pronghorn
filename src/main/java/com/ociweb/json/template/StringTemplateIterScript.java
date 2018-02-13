package com.ociweb.json.template;

import com.ociweb.json.appendable.AppendableByteWriter;

public interface StringTemplateIterScript<T> {
	boolean fetch(AppendableByteWriter apendable, T source, int i);
}

