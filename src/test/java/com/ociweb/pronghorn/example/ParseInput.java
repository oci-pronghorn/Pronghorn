package com.ociweb.pronghorn.example;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.ElapsedTimeRecorder;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class ParseInput extends PronghornStage {

	private TrieParserReader reader;
	private TrieParser parser;
	private final Pipe<RawDataSchema> input;
	private ElapsedTimeRecorder etl;
	private long totalSum;
	private long totalCount;
	
	protected ParseInput(GraphManager graphManager, Pipe<RawDataSchema> input) {
		super(graphManager, input, NONE);
		this.input = input;
	}

	@Override
	public void startup() {
		reader = new TrieParserReader(5);
		parser = new TrieParser(200,4,false,true);
		parser.setUTF8Value("%b\r\n", 1);
		parser.setUTF8Value("OUT%bBusinessLatency:%i µs\r\n", 1000);
		parser.setUTF8Value("OUT%bBusinessLatency:%i ms\r\n", 1000_000);
		parser.setUTF8Value("OUT%bBusinessLatency:%i sec\r\n",1000_000_000);
		parser.setUTF8Value("OUT%bBusinessLatency:%i %b\r\n", 2);
		etl = new ElapsedTimeRecorder();
	}
	
	@Override
	public void run() {
				
	    while (Pipe.hasContentToRead(input)) {
	    	int idx = Pipe.takeMsgIdx(input);
	    	if (idx==-1) {
	    		Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
	    		Pipe.releaseReadLock(input);
	    		
	    		etl.report(System.out);
	    		
	    		long avg = totalSum/totalCount;
	    	
	    		Appendables.appendNearestTimeUnit(System.out.append("avg "), avg).append("\r\n");
	    		
	    		requestShutdown();
	    		return;
	    	} else {
	    		DataInputBlobReader<RawDataSchema> stream = Pipe.openInputStream(input);
	    		
	    		TrieParserReader.parseSetup(reader, stream);
	    		
	    		while (TrieParserReader.parseHasContent(reader)) {
		    		long id = reader.parseNext(parser);
		    		
		    		if (id==-1) {
		    			reader.parseSkipOne();
		    		} else {
		    			if (id==2) {
		    				
		    				long value = reader.capturedLongField(reader, 1);
		    				StringBuilder builder = new StringBuilder();
		    				reader.capturedFieldBytesAsUTF8(reader, 2, builder);
		    				
		    				System.err.println(value+" "+builder);
		    				
		    			} else {
		    				if (id!=1) {
			    						    					
		    					long value = reader.capturedLongField(reader, 1);
			    							    				
			    				//System.out.println(value+" "+id);
			    				
			    				ElapsedTimeRecorder.record(etl, value*id);
			    				
			    				totalSum += (value*id);
			    				totalCount++;
			    				
		    				}				
		    			}
		    		}
	    		}
	    		
	    		
	    		
	    		Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, RawDataSchema.MSG_CHUNKEDSTREAM_1));
	    		Pipe.releaseReadLock(input);	
	    			
	    	}
	    	
	    }
		
	}

}
