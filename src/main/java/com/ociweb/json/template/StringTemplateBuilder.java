package com.ociweb.json.template;

import com.ociweb.json.appendable.AppendableByteWriter;
import com.ociweb.json.appendable.ByteWriter;

public class StringTemplateBuilder<T> implements ByteWriter {
	private StringTemplateScript<T>[] script;
	private int count;

	public StringTemplateBuilder() {
		this.script = new StringTemplateScript[8];
	}

	@Override
	public void write(byte[] b) {
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int pos, int len) {
		add(b, pos, len);
	}

	public StringTemplateBuilder<T> add(String text) {
		return addBytes(text.getBytes());
	}

	public StringTemplateBuilder<T> add(final byte[] byteData) {
		return add(byteData, 0, byteData.length);
	}

	public StringTemplateBuilder<T> add(final byte[] byteData, int pos, int len) {
		if (byteData != null && len > 0) {
			final byte[] localData = new byte[len];
			System.arraycopy(byteData, pos, localData, 0, len);
			addBytes(localData);
		}
		return this;
	}

	public <N> StringTemplateBuilder<T> add(final StringTemplateIterScript<T, N> data) {
		return append(
				new StringTemplateScript<T>() {
					@Override
					public void render(AppendableByteWriter writer, T source) {
						N node = null;
						for(int i = 0; (node = data.render(writer, source, i, node)) != null; i++) {
						}
					}
				});
	}

	public StringTemplateBuilder<T> add(final StringTemplateScript<T>[] data, final StringTemplateBranching<T> branching) {
		final StringTemplateScript<T>[] localData = new StringTemplateScript[data.length];
		System.arraycopy(data, 0, localData, 0, data.length);

		return append(
				new StringTemplateScript<T>() {
					@Override
					public void render(AppendableByteWriter writer, T source) {
						int i = branching.branch(source);
						if (i != -1) { // -1 is no-op
							assert (i < localData.length) : "String template builder selected invalid branch.";
							localData[i].render(writer, source);
						}
					}
				});
	}

	public StringTemplateBuilder<T> add(StringTemplateScript<T> data) {
		return append(data);
	}

	public void render(AppendableByteWriter writer, T source) {
		//assert(immutable) : "String template builder can only be rendered after lock.";
		for(int i=0;i<count;i++) {
			script[i].render(writer, source);
		}
	}

	private StringTemplateBuilder<T> addBytes(final byte[] byteData) {
		return append(
				new StringTemplateScript<T>() {
					@Override
					public void render(AppendableByteWriter writer, T source) {
						writer.write(byteData, 0, byteData.length);
					}
				});
	}

	private StringTemplateBuilder<T> append(StringTemplateScript<T> fetchData) {
		//assert(!immutable) : "String template builder cannot be modified after lock.";
		if (count==script.length) {
			StringTemplateScript<T>[] newScript = new StringTemplateScript[script.length*2];
			System.arraycopy(script, 0, newScript, 0, script.length);
			script = newScript;
		}
		script[count++] = fetchData;
		return this;
	}
}
