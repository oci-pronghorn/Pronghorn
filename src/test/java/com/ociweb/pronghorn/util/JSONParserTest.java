package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.util.parse.JSONParser;
import com.ociweb.pronghorn.util.parse.JSONStreamParser;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitor;
import com.ociweb.pronghorn.util.parse.JSONStreamVisitorCapture;
import com.ociweb.pronghorn.util.parse.JSONVisitor;
import com.ociweb.pronghorn.util.parse.JSONVisitorCapture;

public class JSONParserTest {

	@Test
	public void simpleTest() {
				
		String json = " { \"key\" : \"value\" }  ";
		
		Pipe pipe = buildPopulatedPipe(json);
			
		
		TrieParserReader reader = JSONParser.newReader();
		StringBuilder target = new StringBuilder();
		JSONVisitor visitor = new JSONVisitorCapture(target);
		
		
		int msgIdx = Pipe.takeMsgIdx(pipe);
		JSONParser.parse(pipe, reader, visitor );
		
		assertEquals("{key:value}",target.toString());
	}

	@Test
	public void complexTest() {
				
		String json = " { \"key\" : \"value\" \n, \"key2\" : \"value2\"}  ";
		
		Pipe pipe = buildPopulatedPipe(json);
			
		
		TrieParserReader reader = JSONParser.newReader();
		StringBuilder target = new StringBuilder();
		JSONVisitor visitor = new JSONVisitorCapture(target);		
		
		int msgIdx = Pipe.takeMsgIdx(pipe);
		JSONParser.parse(pipe, reader, visitor );
		
		assertEquals("{key:value,key2:value2}",target.toString());
	}
	
	
	@Test
	public void arrayTest() {
				
		String json = " [ { \"key\" : \"value\" } , \n { \"key\" : \"value\" }     ] ";
		
		Pipe pipe = buildPopulatedPipe(json);
			
		
		TrieParserReader reader = JSONParser.newReader();
		StringBuilder target = new StringBuilder();
		JSONVisitor visitor = new JSONVisitorCapture(target);		
		
		int msgIdx = Pipe.takeMsgIdx(pipe);
		JSONParser.parse(pipe, reader, visitor );
				
		assertEquals("[{key:value},{key:value}]",target.toString());
		
	}
		

	private Pipe buildPopulatedPipe(String json) {
		Pipe pipe = new Pipe(new PipeConfig(RawDataSchema.instance));
		
		pipe.initBuffers();
		int size = Pipe.addMsgIdx(pipe, 0);
		DataOutputBlobWriter output = pipe.outputStream(pipe);
		output.openField();
		output.append(json);
		output.closeLowLevelField();
		Pipe.confirmLowLevelWrite(pipe, size);
		Pipe.publishWrites(pipe);
		return pipe;
	}
	
	@Test
	public void streamingArrayTest() {
				
		String json = " [ { \"key\" : \"value\" } , \n { \"key\" : \"value\" }     ] ";
		
		Pipe pipe = buildPopulatedPipe(json);
			
		
		TrieParserReader reader = new TrieParserReader(2);
		
		int msgIdx = Pipe.takeMsgIdx(pipe);
		TrieParserReader.parseSetup(reader,pipe); 
		
		StringBuilder target = new StringBuilder();
		
		JSONStreamVisitor visitor = new JSONStreamVisitorCapture(target);		
		
		Pipe.takeMsgIdx(pipe);
		JSONStreamParser parser = new JSONStreamParser();
		parser.parse(reader, visitor);
				
		assertEquals( target.toString().replaceAll("\n", "\\n"),				
				"[{\n    \"key\":\"value\"}\n,\n{\n    \"key\":\"value\"}\n]",target.toString());
		
		 
		
	}

}



