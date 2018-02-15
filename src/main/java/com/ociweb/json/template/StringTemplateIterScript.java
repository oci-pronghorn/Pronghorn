package com.ociweb.json.template;

import com.ociweb.json.appendable.AppendableByteWriter;

public interface StringTemplateIterScript<T, N> {
	N fetch(AppendableByteWriter appendable, T source, int i, N node);
}

