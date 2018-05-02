package com.ociweb.json.decode;

import com.ociweb.json.JSONExtractor;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public class JSONReader extends JSONTable<JSONReader> {
    private boolean locked = false;

    public JSONReader() {
        super(new JSONExtractor());
    }

    public JSONReader(boolean writeDot) {
        super(new JSONExtractor(writeDot));
    }

    public boolean isLocked() {
        return locked;
    }

    @Override
    JSONReader tableEnded() {
        locked = true;
        return this;
    }

    TrieParser trieParser() {
        return extractor.trieParser();
    }

    JSONStreamVisitorToChannel newJSONVisitor() {
        return extractor.newJSONVisitor();
    }

    void addToStruct(StructRegistry schema, int structId) {
        extractor.addToStruct(schema, structId);
    }

    int[] getIndexPositions() {
        return extractor.getIndexPositions();
    }
}
