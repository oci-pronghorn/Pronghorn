package com.ociweb.json;

public interface JSONExtractorActive {

	JSONExtractorActive array();
	JSONExtractorActive key(String name);
	JSONExtractorUber completePath(String pathName);
	JSONExtractorUber completePath(String pathName, Object association);
		
}
