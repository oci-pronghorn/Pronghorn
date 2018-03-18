package com.ociweb.json;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.ociweb.pronghorn.struct.BStructSchema;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.parse.JSONFieldMapping;
import com.ociweb.pronghorn.util.parse.JSONFieldSchema;
import com.ociweb.pronghorn.util.parse.JSONReader;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public class JSONExtractor implements JSONExtractorCompleted, JSONExtractorActive {

	private final JSONFieldSchema schema;
	private JSONFieldMapping activeMapping;
	private List<String> path;
	private boolean writeDot;
	
	public JSONExtractor() {
		schema = new JSONFieldSchema(0);//can we set the position here for the null block???=
		writeDot = false;
	}
	
	public JSONExtractor(boolean writeDot) {
		this.schema = new JSONFieldSchema(0);
		this.writeDot = writeDot;
	}
	
	public int toStruct(BStructSchema struct) {
		return schema.toStruct(struct);

	}
	
    @Override
	public JSONReader reader() {
		return schema; //clone if baseoffset is different??
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
	
	public JSONExtractorActive newPath(JSONType type) {
		return newPath(type, false);
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
	
	public JSONExtractorCompleted completePath(String pathName) { //can only call newPath next
		
		activeMapping.setName(pathName);
		activeMapping.setPath(schema, path.toArray(new String[path.size()]));	
		schema.addMappings(activeMapping);
		return this;
	}

	
}
