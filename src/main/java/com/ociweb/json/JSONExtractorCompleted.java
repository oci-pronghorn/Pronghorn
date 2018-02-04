package com.ociweb.json;

import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public interface JSONExtractorCompleted {

	JSONExtractorActive newPath(JSONType type, boolean isAligned);
	TrieParser trieParser();
	JSONStreamVisitorToChannel newJSONVisitor();
}
