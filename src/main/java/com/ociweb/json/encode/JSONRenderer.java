package com.ociweb.json.encode;

import com.ociweb.json.template.StringTemplateBuilder;
import com.ociweb.json.appendable.AppendableByteWriter;

public class JSONRenderer<T> extends JSONRoot<T, JSONRenderer<T>> {

    public JSONRenderer() {
        super(new StringTemplateBuilder<>(), new JSONKeywords(), 0);
    }

    public JSONRenderer(JSONKeywords keywords) {
        super(new StringTemplateBuilder<>(), keywords, 0);
    }

    public void render(AppendableByteWriter writer, T source) {
        builder.render(writer, source);
    }


}
