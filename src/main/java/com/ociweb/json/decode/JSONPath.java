package com.ociweb.json.decode;

import com.ociweb.json.JSONExtractorActive;

public abstract class JSONPath<P> {
    private JSONExtractorActive extractor;

    JSONPath(JSONExtractorActive extractor) {
        this.extractor = extractor;
    }


    public P asField(String extractionPath, String name) {
        extractor.completePath(extractionPath, name);
        extractor = null;
        return pathEnded();
    }

    public P asField(String extractionPath, String name, Object object) {
        extractor.completePath(extractionPath, name, object);
        extractor = null;
        return pathEnded();
    }

    public <T extends Enum<T>> P asField(String extractionPath, T field) {
        extractor.completePath(extractionPath, field.name(), field);
        extractor = null;
        return pathEnded();
    }

    abstract P pathEnded();
}
