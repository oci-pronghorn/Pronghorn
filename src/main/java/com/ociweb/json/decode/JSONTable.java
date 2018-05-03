package com.ociweb.json.decode;

import com.ociweb.json.JSONAccumRule;
import com.ociweb.json.JSONExtractor;
import com.ociweb.json.JSONExtractorActive;
import com.ociweb.json.JSONType;

public abstract class JSONTable<P> {
    final JSONExtractor extractor;

    JSONTable(JSONExtractor extractor) {
        this.extractor = extractor;
    }

    public JSONPath<JSONTable<P>> element(JSONType type) {
        return element(type, false, null);
    }

    public JSONPath<JSONTable<P>> element(JSONType type, boolean isAligned) {
        return element(type, isAligned, null);
    }

    public JSONPath<JSONTable<P>> element(JSONType type, boolean isAligned, JSONAccumRule accumRule) {
        JSONExtractorActive active = extractor.newPath(type, isAligned, accumRule);
        return new JSONPath<JSONTable<P>>(active) {
            @Override
            JSONTable <P> pathEnded() {
                return JSONTable.this;
            }
        };
    }

    public P finish() {
        return tableEnded();
    }

    abstract P tableEnded();
}

