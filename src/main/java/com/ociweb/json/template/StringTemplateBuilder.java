package com.ociweb.json.template;

import com.ociweb.json.appendable.AppendableByteWriter;
import com.ociweb.json.appendable.ByteWriter;

public class StringTemplateBuilder<T> implements ByteWriter {
	private StringTemplateScript[] script;
	private int count;
	private boolean immutable = false;

	public StringTemplateBuilder() {
		this.script = new StringTemplateScript[8];
	}

	public void lock() {
		immutable = true;
	}

	public StringTemplateBuilder<T> add(String text) {
		addBytes(text.getBytes());
		return this;
	}

	@Override
	public void write(byte[] b, int pos, int len) {
		add(b, pos, len);
	}

	@Override
	public void write(byte[] b) {
		write(b, 0, b.length);
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

	private void addBytes(final byte[] byteData) {
		append(
			new StringTemplateScript<T>() {
				@Override
				public void fetch(AppendableByteWriter writer, T source) {
					writer.write(byteData, 0, byteData.length);
				}
			});
	}

	public <N> StringTemplateBuilder<T> add(final StringTemplateIterScript<T, N> data) {
		append(
				new StringTemplateScript<T>() {
					@Override
					public void fetch(AppendableByteWriter writer, T source) {
						N node = null;
						for(int i = 0; (node = data.fetch(writer, source, i, node)) != null; i++) {
						}
					}
				});
		return this;
	}

	public StringTemplateBuilder<T> add(final StringTemplateBuilder<T>[] data, final StringTemplateBranching<T> branching) {
		final StringTemplateBuilder<T>[] localData = new StringTemplateBuilder[data.length];
		System.arraycopy(data, 0, localData, 0, data.length);
		append(
				new StringTemplateScript<T>() {
					@Override
					public void fetch(AppendableByteWriter writer, T source) {
						int i = branching.branch(source);
						assert(i < localData.length) : "String template builder selected invalid branch.";
						localData[i].render(writer, source);
					}
				});
		return this;
	}

	public StringTemplateBuilder<T> add(StringTemplateScript<T> data) {
		append(data);
		return this;
	}

	public void render(AppendableByteWriter writer, T source) {
		assert(immutable) : "String template builder can only be rendered after lock.";
		for(int i=0;i<count;i++) {
			script[i].fetch(writer, source);
		}
	}

	private void append(StringTemplateScript<T> fetchData) {
		assert(!immutable) : "String template builder cannot be modified after lock.";
		if (count==script.length) {
			StringTemplateScript[] newScript = new StringTemplateScript[script.length*2];
			System.arraycopy(script, 0, newScript, 0, script.length);
			script = newScript;
		}
		script[count++] = fetchData;
	}
}
