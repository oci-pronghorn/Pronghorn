package com.ociweb.json;

public interface JSONExtractorUber extends  JSONExtractorCompleted {
	JSONExtractorActive newPath(JSONType type, boolean isAligned, JSONAccumRule accumRule);
	JSONExtractorActive newPath(JSONType type, boolean isAligned);
	JSONExtractorActive newPath(JSONType type);
}
