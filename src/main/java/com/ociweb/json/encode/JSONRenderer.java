package com.ociweb.json.encode;

import com.ociweb.json.template.StringTemplateBuilder;
import com.ociweb.json.appendable.AppendableByteWriter;

public class JSONRenderer<T> extends JSONRoot<T, JSONRenderer<T>> {
    public JSONRenderer() {
        super(new StringTemplateBuilder<T>(), new JSONKeywords(), 0);
    }

    public JSONRenderer(JSONKeywords keywords) {
        super(new StringTemplateBuilder<T>(), keywords, 0);
    }

    public boolean isLocked() {
        return this.builder.isLocked();
    }

    public void render(AppendableByteWriter writer, T source) {
        builder.render(writer, source);
    }

    @Override
    JSONRenderer<T> rootEnded() {
        return this;
    }
}
