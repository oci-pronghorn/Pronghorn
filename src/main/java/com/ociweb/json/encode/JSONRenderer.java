package com.ociweb.json.encode;

import com.ociweb.json.template.StringTemplateBuilder;
import com.ociweb.json.appendable.AppendableByteWriter;

public class JSONRenderer<T> extends JSONRoot<T, T, JSONRenderer<T>> {
    public JSONRenderer() {
        super(new JSONBuilder<>());
    }

    public JSONRenderer(JSONKeywords keywords) {
        super(new JSONBuilder<>(new StringTemplateBuilder<T>(), keywords, 0, null));
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
/* TODO: will need
    @Override
    public <M> JSONRenderer<T> recurseRoot(ToMemberFunction<T, T> accessor) {
        assert(false) : "Renderer cannot recurse self";
        return null;
    }
*/
}
