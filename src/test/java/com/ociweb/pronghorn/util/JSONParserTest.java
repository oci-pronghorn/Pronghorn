package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class JSONParserTest {

	@Test
	public void simpleTest() {
				
		String json = " { \"key\" : \"value\" }  ";
		
		Pipe pipe = buildPopulatedPipe(json);
			
		
		TrieParserReader reader = JSONParser.newReader();
		StringBuilder target = new StringBuilder();
		JSONVisitor<StringBuilder> visitor = visitor(target);
		
		
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
		JSONVisitor<StringBuilder> visitor = visitor(target);
		
		
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
		JSONVisitor<StringBuilder> visitor = visitor(target);
		
		
		int msgIdx = Pipe.takeMsgIdx(pipe);
		JSONParser.parse(pipe, reader, visitor );
				
		assertEquals("[{key:value},{key:value}]",target.toString());
		
	}
	
	
	private <A extends Appendable> JSONVisitor<A> visitor(final A target) {
		JSONVisitor<A> visitor = new JSONVisitor<A>() {

			
			@Override
			public A stringValue() {
				return target;
			}

			@Override
			public void stringValueComplete() {
				
			}

			@Override
			public A stringName(int fieldIndex) {
				if (0!=fieldIndex) {
					try {
						target.append(",");
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}				
				return target;
			}

			@Override
			public void stringNameComplete() {
				try{
				target.append(':');
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}

			@Override
			public void arrayBegin() {
				try{
				target.append('[');
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}

			@Override
			public void arrayEnd() {
				try{
				target.append(']');
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}

			@Override
			public void arrayIndexBegin(int instance) {
				if (0!=instance) {
					try {
						target.append(",");
					} catch (Exception ex) {
						throw new RuntimeException(ex);
					}
				}
			}

			@Override
			public void numberValue(long m, byte e) {
				Appendables.appendDecimalValue(target, m, e);
			}

			@Override
			public void nullValue() {
				try {
				target.append("null");
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}

			@Override
			public void booleanValue(boolean b) {
				try{
				target.append(Boolean.toString(b));
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}

			@Override
			public void objectEnd() {
				try{
				target.append('}');
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}

			@Override
			public void objectBegin() {
				try{
				target.append('{');
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}
			
		};
		return visitor;
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
	
	
	
}



