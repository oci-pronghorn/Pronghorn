package com.ociweb.json;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.ociweb.pronghorn.struct.StructBuilder;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.util.CharSequenceToUTF8Local;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.TrieParserReaderLocal;
import com.ociweb.pronghorn.util.parse.JSONFieldMapping;
import com.ociweb.pronghorn.util.parse.JSONFieldSchema;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorToChannel;

public class JSONExtractorImpl implements JSONExtractorUber, JSONExtractorActive {

	private final JSONFieldSchema schema;
	private JSONFieldMapping activeMapping;
	private List<CharSequence> path;
	private boolean writeDot;
	
	private int structId;
	private int[] indexLookup;
	
	private static TrieParser parser;
	private static final int keyToken = 1;
	private static final int dotToken = 2;
	private static final int arrayToken = 3;

	static {
		parser = new TrieParser();
		parser.setUTF8Value("%b.", keyToken);
		parser.setUTF8Value(".", dotToken);
		parser.setUTF8Value("[].", arrayToken);
	}
	
	public JSONExtractorImpl() {
		schema = new JSONFieldSchema(0);//can we set the position here for the null block???=
		writeDot = false;
	}
	
	public JSONExtractorImpl(boolean writeDot) {
		this.schema = new JSONFieldSchema(0);
		this.writeDot = writeDot;
	}
	

	private void parseExtractionPath(String extractionPath) {
		JSONExtractorActive running = this;
		TrieParserReader trieParserReader = TrieParserReaderLocal.get();
				
		CharSequenceToUTF8Local.get()
		   .convert(extractionPath)
		   .append(".")
		   .parseSetup(trieParserReader);
		
		int token = 0;
		while ((token = (int)trieParserReader.parseNext(parser)) != -1) {
			switch(token) {
				case keyToken:
					path.add(TrieParserReader.capturedFieldBytesAsUTF8(trieParserReader, 0, new StringBuilder()));
					break;
				case dotToken:
					break;
				case arrayToken:
					path.add("[]");
					break;
			}
		}
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
		return newPath(type, JSONAligned.UNPADDED);
	}
	
	public JSONExtractorActive newPath(JSONType type, JSONAligned isAligned) {
		
		activeMapping = new JSONFieldMapping(schema, type, isAligned);
		if (path==null) {
			path = new ArrayList<CharSequence>();
		} else {
			path.clear();
		}
		return this;
	}
	
	public JSONExtractorActive newPath(JSONType type, 
			                           JSONAligned isAligned, 
			                           JSONAccumRule accumRule) {
		
		activeMapping = new JSONFieldMapping(schema, type, isAligned, accumRule);
		if (path==null) {
			path = new ArrayList<CharSequence>();
		} else {
			path.clear();
		}
		return this;
	}
	
	@Override
	public JSONExtractorUber completePath(String extractionPath, String pathName) { //can only call newPath next
		
		parseExtractionPath(extractionPath);
		activeMapping.setName(pathName);
		activeMapping.setPath(schema, path.toArray(new CharSequence[path.size()]));	
		schema.addMappings(activeMapping);
		return this;
	}

	@Override
	public JSONExtractorUber completePath(String extractionPath, String pathName, Object optionalAssociation) {
		
		parseExtractionPath(extractionPath);
		activeMapping.setName(pathName);
		activeMapping.setPath(schema, path.toArray(new CharSequence[path.size()]));	
		schema.addMappings(activeMapping);
		activeMapping.setAssociatedObject(optionalAssociation);
		
		return this;
	}

	@Override
	public JSONExtractorUber completePath(String extractionPath, String pathName, Object optionalAssociation, JSONRequired required, Object validator) {
		
		parseExtractionPath(extractionPath);
		activeMapping.setName(pathName);
		activeMapping.setPath(schema, path.toArray(new CharSequence[path.size()]));	
		schema.addMappings(activeMapping);
		activeMapping.setAssociatedObject(optionalAssociation);
		activeMapping.setValidator(required, validator);
		
		return this;
	}
	
	@Override
	public void addToStruct(StructRegistry typeData, int structId) {
		assert(null==indexLookup) : "can only be called once";
		this.indexLookup = schema.addToStruct(typeData, structId);
		this.structId = structId;
	}

	@Override
	public int[] getIndexPositions() {
		assert(null!=indexLookup) : "expected there to be some fields to be extracted, non found";
		return indexLookup;
	}

    @Override
    public int getStructId() {
    	return structId;
    }

	public void addToStruct(StructRegistry typeData, StructBuilder structBuilder) {
		schema.addToStruct(typeData, structBuilder);
	}
    
	
}
