package com.ociweb.json;

public interface JSONExtractorActive {

	JSONExtractorActive array();
	JSONExtractorActive key(String name);
	JSONExtractorCompleted completePath(String pathName);
	<T extends Enum<T>> JSONExtractorCompleted completePath(T pathObject);
	JSONExtractorCompleted completePath(String pathName, Object association);
		
}
