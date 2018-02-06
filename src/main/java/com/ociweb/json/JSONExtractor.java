package com.ociweb.json;

import java.io.File;
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
	private boolean writeDot = false;
	
	public JSONExtractor() {
		schema = new JSONFieldSchema();
	}

	public TrieParser trieParser() {
		if (writeDot) {
				
			File createTempFile;
			try {
				
				createTempFile = File.createTempFile("parser", ".dot");
				schema.parser().toDOTFile(createTempFile);
				
				String absPath = createTempFile.getAbsolutePath();
				System.err.println("file: "+absPath);
				
				String filename = createTempFile.getName();
				String command = "dot -Tsvg -o"+filename+".svg "+filename;
				System.err.println(command);
//	            //to produce the png we must call
//	            //  dot -Tpng -O deviceGraph.dot        
//				Process result = Runtime.getRuntime()
//	            		.exec(command);
//	            
//	            // dot -Tsvg -oparser2030661959993832585.dot.svg parser2030661959993832585.dot
//	            
//	            if (0!=result.waitFor()) {
//	                System.err.println("error");
//	            }
	           
				
				
			} catch (Throwable t) {
				t.printStackTrace();
			}
			
			
			
		}
		
		writeDot = false;
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
