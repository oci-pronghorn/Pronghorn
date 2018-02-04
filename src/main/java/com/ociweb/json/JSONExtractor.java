package com.ociweb.json;

import java.util.ArrayList;
import java.util.List;

import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.parse.JSONFieldMapping;
import com.ociweb.pronghorn.util.parse.JSONFieldSchema;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public class JSONExtractor implements JSONExtractorCompleted, JSONExtractorActive {

	private final JSONFieldSchema schema;
	private JSONFieldMapping activeMapping;
	private List<String> path;
	
	public JSONExtractor() {
		schema = new JSONFieldSchema();
	}

	public TrieParser trieParser() {
		return schema.parser();
	}
	
	public JSONStreamVisitorToChannel newJSONVisitor() {
		return new JSONStreamVisitorToChannel(schema);
	}
	
	
	public JSONExtractorActive newPath(JSONType type, boolean isAligned) {
		
		activeMapping = new JSONFieldMapping(schema, type, isAligned);
		if (path==null) {
			path = new ArrayList<String>();
		} else {
			path.clear();
		}
		return this;
	}
	
	public JSONExtractorActive newPath(JSONType type, 
			                           boolean isAligned, 
			                           JSONAccumRule accumRule) {
		
		activeMapping = new JSONFieldMapping(schema, type, isAligned, accumRule);
		if (path==null) {
			path = new ArrayList<String>();
		} else {
			path.clear();
		}
		return this;
	}
	
	public JSONExtractorActive array() {		
		path.add("[]");		
		return this;
	}
	
	public JSONExtractorActive key(String name) {
		if (!"[]".equals(name)) {
			path.add(name);
		} else {
			array();
		}
		return this;
	}
	
	public JSONExtractorCompleted completePath() { //can only call newPath next
		
		activeMapping.setPath(schema, path.toArray(new String[path.size()]));	
		schema.addMappings(activeMapping);
		return this;
	}
	
}
