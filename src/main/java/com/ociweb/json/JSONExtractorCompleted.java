package com.ociweb.json;

import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public interface JSONExtractorCompleted {
	TrieParser trieParser();
	JSONStreamVisitorToChannel newJSONVisitor();
	void addToStruct(StructRegistry schema, int structId);
	int[] getIndexPositions();
}

