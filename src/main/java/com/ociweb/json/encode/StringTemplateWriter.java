package com.ociweb.json.encode;

public interface StringTemplateWriter extends Appendable {
	void write(byte b[]);

	Appendable append(CharSequence csq);

	Appendable append(CharSequence csq, int start, int end);

	Appendable append(char c);
}

