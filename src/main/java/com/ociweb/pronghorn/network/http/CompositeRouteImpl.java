package com.ociweb.pronghorn.network.http;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.json.JSONExtractorCompleted;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.util.TrieParserReader;

public class CompositeRouteImpl implements CompositePath {

	//TODO: move this entire logic into HTTP1xRouterStageConfig to eliminate this object construction.
	private final JSONExtractorCompleted extractor; 
	private final URLTemplateParser parser; 
	private final IntHashTable headerTable;
	private final int groupId;
	private final AtomicInteger pathCounter;
	private final HTTP1xRouterStageConfig<?,?,?,?> config;
	private final ArrayList<FieldExtractionDefinitions> defs;
	private final TrieParserReader reader = new TrieParserReader(4,true);
    	
	public CompositeRouteImpl(HTTP1xRouterStageConfig<?,?,?,?> config,
			                  JSONExtractorCompleted extractor, 
			                  URLTemplateParser parser, 
			                  IntHashTable headerTable,
			                  int groupId,
			                  AtomicInteger pathCounter) {
		
		this.defs = new ArrayList<FieldExtractionDefinitions>();
		this.config = config;
		this.extractor = extractor;
		this.parser = parser;
		this.headerTable = headerTable;
		this.groupId = groupId;
		this.pathCounter = pathCounter;
		
	}

	@Override
	public int routeId() {
		return groupId;
	}

	@Override
	public CompositePath path(CharSequence path) {
		
		int pathsId = pathCounter.getAndIncrement();
		
		FieldExtractionDefinitions fieldExDef = parser.addPath(path, groupId, pathsId);//hold for defaults..
		config.storeRequestExtractionParsers(pathsId, fieldExDef); //this looked up by pathId
		config.storeRequestedJSONMapping(pathsId, extractor);
		config.storeRequestedHeaders(pathsId, headerTable);		
		defs.add(fieldExDef);
		
		return this;
	}
	
	@Override
	public CompositeRoute defaultInteger(String key, long value) {
		
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultInteger(reader, key, value);			
		}
		return this;
	}

	@Override
	public CompositeRoute defaultText(String key, String value) {
		
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultText(reader, key, value);			
		}
		return this;
	}

	@Override
	public CompositeRoute defaultDecimal(String key, long m, byte e) {
		
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultDecimal(reader, key, m, e);			
		}
		return this;
	}

	@Override
	public CompositeRoute defaultRational(String key, long numerator, long denominator) {
		
		int i = defs.size();
		while (--i>=0) {
			defs.get(i).defaultRational(reader, key, numerator, denominator);			
		}
		return this;
	}


}
