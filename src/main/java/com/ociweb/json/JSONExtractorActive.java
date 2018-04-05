package com.ociweb.json;

public interface JSONExtractorActive {

	JSONExtractorActive array();
	JSONExtractorActive key(String name);
	JSONExtractorCompleted completePath(String pathName);
	JSONExtractorCompleted completePath(String pathName, Object association);
		
}
