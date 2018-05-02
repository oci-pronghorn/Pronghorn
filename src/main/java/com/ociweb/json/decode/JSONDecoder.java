package com.ociweb.json.decode;

import com.ociweb.json.JSONExtractor;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public class JSONDecoder {
    private final JSONExtractor extractor;
    private boolean locked = false;

    public JSONDecoder() {
        extractor = new JSONExtractor();
    }

    public JSONDecoder(boolean writeDot) {
        extractor = new JSONExtractor(writeDot);
    }

    public JSONTable<JSONDecoder> begin() {
        return new JSONTable<JSONDecoder>(extractor) {
            public JSONDecoder tableEnded() {
                locked = true;
                return JSONDecoder.this;
            }
        };
    }

    public boolean isLocked() {
        return locked;
    }

    public TrieParser trieParser() {
        return extractor.trieParser();
    }

    public JSONStreamVisitorToChannel newJSONVisitor() {
        return extractor.newJSONVisitor();
    }

    public void addToStruct(StructRegistry schema, int structId) {
        extractor.addToStruct(schema, structId);
    }

    public int[] getIndexPositions() {
        return extractor.getIndexPositions();
    }
}
