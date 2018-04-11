package com.ociweb.json;

import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public interface JSONExtractorCompleted {

	JSONExtractorActive newPath(JSONType type, boolean isAligned, JSONAccumRule accumRule);
	JSONExtractorActive newPath(JSONType type, boolean isAligned);
	JSONExtractorActive newPath(JSONType type);
	TrieParser trieParser();
	JSONStreamVisitorToChannel newJSONVisitor();
	int[] indexTable(StructRegistry schema, int structId);
	void addToStruct(StructRegistry schema, int structId);
}
