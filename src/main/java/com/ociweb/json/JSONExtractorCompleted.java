package com.ociweb.json;

import com.ociweb.pronghorn.struct.BStructSchema;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.parse.JSONReader;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public interface JSONExtractorCompleted {

	JSONExtractorActive newPath(JSONType type, boolean isAligned, JSONAccumRule accumRule);
	JSONExtractorActive newPath(JSONType type, boolean isAligned);
	JSONExtractorActive newPath(JSONType type);
	TrieParser trieParser();
	JSONStreamVisitorToChannel newJSONVisitor();
	JSONReader reader();
	int[] indexTable(BStructSchema schema, int structId);
	void addToStruct(BStructSchema schema, int structId);
}
