package com.ociweb.json;

public interface JSONExtractorUber extends  JSONExtractorCompleted {
	JSONExtractorActive newPath(JSONType type, JSONAligned isAligned, JSONAccumRule accumRule);
	JSONExtractorActive newPath(JSONType type, JSONAligned isAligned);
	JSONExtractorActive newPath(JSONType type);
}
