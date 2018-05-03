package com.ociweb.json.decode;

import com.ociweb.json.JSONExtractor;
import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public class JSONDecoder implements JSONExtractorCompleted {
    private final JSONExtractor extractor;
    private boolean locked = false;

    public JSONDecoder() {
        extractor = new JSONExtractor();
    }

    public JSONDecoder(boolean writeDot) {
        extractor = new JSONExtractor(writeDot);
    }

    public JSONTable<JSONDecoder> begin() {
        assert(!locked) : "Cannot begin a locked decoder";
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

    @Override
    public TrieParser trieParser() {
        return extractor.trieParser();
    }

    @Override
    public JSONStreamVisitorToChannel newJSONVisitor() {
        return extractor.newJSONVisitor();
    }

    @Override
    public void addToStruct(StructRegistry schema, int structId) {
        extractor.addToStruct(schema, structId);
    }

    @Override
    public int[] getIndexPositions() {
        return extractor.getIndexPositions();
    }
}
