package com.ociweb.json.decode;

import com.ociweb.json.JSONExtractorImpl;
import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.struct.StructBuilder;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public class JSONExtractor implements JSONExtractorCompleted {
    private final JSONExtractorImpl extractor;
    private boolean locked = false;

    public JSONExtractor() {
        extractor = new JSONExtractorImpl();
    }

    public JSONExtractor(boolean writeDot) {
        extractor = new JSONExtractorImpl(writeDot);
    }

    public JSONTable<JSONExtractor> begin() {
        assert(!locked) : "Cannot begin a locked decoder";
        return new JSONTable<JSONExtractor>(extractor) {
            public JSONExtractor tableEnded() {
                locked = true;
                return JSONExtractor.this;
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

    public void addToStruct(StructRegistry schema, StructBuilder structBuilder) {
    	extractor.addToStruct(schema, structBuilder);
    }

    @Override
    public int[] getIndexPositions() {
        return extractor.getIndexPositions();
    }
    
    @Override
    public int getStructId() {
    	return extractor.getStructId();
    }

	@Override
	public void debugSchema() {
		extractor.debugSchema();
	}

}
