package com.ociweb.json.decode;

import com.ociweb.json.JSONExtractorActive;

public abstract class JSONPath <P> {
    private JSONExtractorActive extractor;

    JSONPath(JSONExtractorActive extractor) {
        this.extractor = extractor;
    }

    public JSONPath key(String key) {
        extractor = extractor.key(key);
        return this;
    }

    public JSONPath array() {
        extractor = extractor.array();
        return this;
    }

    public P asField(String name) {
        extractor.completePath(name);
        extractor = null;
        return pathEnded();
    }

    public P asField(String name, Object object) {
        extractor.completePath(name, object);
        extractor = null;
        return pathEnded();
    }

    public <T extends Enum<T>> P asField(T field) {
        extractor.completePath(field.name(), field);
        extractor = null;
        return pathEnded();
    }

    abstract P pathEnded();

}
