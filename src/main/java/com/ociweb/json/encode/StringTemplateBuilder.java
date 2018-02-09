package com.ociweb.json.encode;

public class StringTemplateBuilder<T> {
	private StringTemplateScript[] script;
	private int count;

	public StringTemplateBuilder() {
		this.script = new StringTemplateScript[8];
	}

	public StringTemplateBuilder<T> add(String text) {
		return add(text.getBytes());
	}

	public StringTemplateBuilder<T> add(final byte[] byteData) {
		append(
				new StringTemplateScript<T>() {
					@Override
					public void fetch(StringTemplateWriter writer, T source) {
						writer.write(byteData);
					}
				});
		return this;
	}

	public StringTemplateBuilder<T> add(StringTemplateScript<T> data) {
		append(data);
		return this;
	}

	public void render(StringTemplateWriter writer, T source) {
		for(int i=0;i<count;i++) {
			script[i].fetch(writer, source);
		}
	}

	private void append(StringTemplateScript<T> fetchData) {
		if (count==script.length) {
			StringTemplateScript[] newScript = new StringTemplateScript[script.length*2];
			System.arraycopy(script, 0, newScript, 0, script.length);
			script = newScript;
		}
		script[count++] = fetchData;
	}
}
