package com.ociweb.json.encode.template;

import com.ociweb.json.encode.appendable.AppendableByteWriter;

public interface StringTemplateScript<T> {
	void fetch(AppendableByteWriter apendable, T source);
}
