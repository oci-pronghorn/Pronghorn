package com.ociweb.json.template;

import com.ociweb.json.appendable.AppendableByteWriter;

public interface StringTemplateScript<T> {
	void fetch(AppendableByteWriter appendable, T source);
}
