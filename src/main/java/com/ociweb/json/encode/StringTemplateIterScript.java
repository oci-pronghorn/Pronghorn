package com.ociweb.json.encode;

public interface StringTemplateIterScript<T> {
	boolean fetch(StringTemplateWriter apendable, T source, int i);
}

