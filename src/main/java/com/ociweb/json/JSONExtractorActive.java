package com.ociweb.json;

public interface JSONExtractorActive {
	
	JSONExtractorUber completePath(String extractionPath, String pathName);
	JSONExtractorUber completePath(String extractionPath, String pathName, Object association);
	JSONExtractorUber completePath(String extractionPath, String pathName, Object association, Object validator);
	
		
}
