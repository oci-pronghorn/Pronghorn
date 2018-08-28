package com.ociweb.json.encode;

import com.ociweb.pronghorn.util.AppendableByteWriter;

public class JSONRenderer<T> extends JSONRoot<T, T, JSONRenderer<T>> {
    private boolean locked = false;

    public JSONRenderer() {
        super(new JSONBuilder<T, T>());
    }

    public JSONRenderer(JSONKeywords keywords) {
        super(new JSONBuilder<T, T>(keywords));
    }

    public boolean isLocked() {
        return locked;
    }

    public void render(AppendableByteWriter writer, T source) {
        assert(locked) : "JSONRenderers can only be rendered once locked";
        builder.render(writer, source);
    }

    @Override
    JSONRenderer<T> rootEnded() {
        locked = true;
        return this;
    }

}
