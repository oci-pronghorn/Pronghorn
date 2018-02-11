package com.ociweb.json.encode;

public class StringTemplateBuilder<T> {
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
				public void fetch(StringTemplateWriter writer, T source) {
					writer.write(byteData);
				}
			});
	}

	public StringTemplateBuilder<T> add(final StringTemplateIterScript<T> data) {
		append(
				new StringTemplateScript<T>() {
					@Override
					public void fetch(StringTemplateWriter writer, T source) {
						int i = 0;
						while (data.fetch(writer, source, i)) {
							i++;
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
					public void fetch(StringTemplateWriter writer, T source) {
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

	public void render(StringTemplateWriter writer, T source) {
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
