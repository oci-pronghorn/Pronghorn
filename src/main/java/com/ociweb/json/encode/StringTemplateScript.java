package com.ociweb.json.encode;

public interface StringTemplateScript<T> {
	void fetch(StringTemplateWriter apendable, T source);
}
