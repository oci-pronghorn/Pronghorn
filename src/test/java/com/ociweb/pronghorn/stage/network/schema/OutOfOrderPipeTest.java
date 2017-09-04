package com.ociweb.pronghorn.stage.network.schema;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.pronghorn.network.schema.TwitterEventSchema;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class OutOfOrderPipeTest {

	
	@Test
	public void pipeDataTest() {
		
		
		Pipe pipe = TwitterEventSchema.instance.newPipe(10, 1000);
		pipe.initBuffers();
		
		PipeWriter.blockWriteFragment(pipe, TwitterEventSchema.MSG_USERPOST_101);
		
		PipeWriter.writeUTF8(pipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_CREATEDAT_57, "this is todays date");
		PipeWriter.writeUTF8(pipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_LANGUAGE_60, "en");
		PipeWriter.writeUTF8(pipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_NAME_52, "name");
		
		PipeWriter.publishWrites(pipe);
		
		boolean ok = PipeReader.tryReadFragment(pipe);
		assertTrue(ok);
		
		String name = PipeReader.readUTF8(pipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_NAME_52, new StringBuilder()).toString();
		String lang = PipeReader.readUTF8(pipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_LANGUAGE_60, new StringBuilder()).toString();
		String create = PipeReader.readUTF8(pipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_CREATEDAT_57, new StringBuilder()).toString();
		
		assertEquals("name",name);
		assertEquals("en",lang);
		assertEquals("this is todays date",create);
		
		
	}
	
	@Test
	public void pipeDataTest2() {
		
		
		Pipe pipe = TwitterEventSchema.instance.newPipe(10, 1000);
		pipe.initBuffers();
		DataOutputBlobWriter writer = PipeWriter.outputStream(pipe);

		PipeWriter.blockWriteFragment(pipe, TwitterEventSchema.MSG_USERPOST_101);
		//PipeWriter.tryWriteFragment(pipe, TwitterEventSchema.MSG_USERPOST_101);
		
		DataOutputBlobWriter.openField(writer);
		writer.writeUTF8Text("this is todays date");
		DataOutputBlobWriter.closeHighLevelField(writer, TwitterEventSchema.MSG_USERPOST_101_FIELD_CREATEDAT_57);
				
		DataOutputBlobWriter.openField(writer);
		writer.writeUTF8Text("en");
		DataOutputBlobWriter.closeHighLevelField(writer, TwitterEventSchema.MSG_USERPOST_101_FIELD_LANGUAGE_60);
		
		DataOutputBlobWriter.openField(writer);
		writer.writeUTF8Text("name");
		DataOutputBlobWriter.closeHighLevelField(writer, TwitterEventSchema.MSG_USERPOST_101_FIELD_NAME_52);
		
		PipeWriter.publishWrites(pipe);

		PipeWriter.tryWriteFragment(pipe, TwitterEventSchema.MSG_USERPOST_101);
		
		DataOutputBlobWriter.openField(writer);
		writer.writeUTF8Text("2 this is todays date");
		DataOutputBlobWriter.closeHighLevelField(writer, TwitterEventSchema.MSG_USERPOST_101_FIELD_CREATEDAT_57);
				
		DataOutputBlobWriter.openField(writer);
		writer.writeUTF8Text("2 en");
		DataOutputBlobWriter.closeHighLevelField(writer, TwitterEventSchema.MSG_USERPOST_101_FIELD_LANGUAGE_60);
		
		DataOutputBlobWriter.openField(writer);
		writer.writeUTF8Text("2 name");
		DataOutputBlobWriter.closeHighLevelField(writer, TwitterEventSchema.MSG_USERPOST_101_FIELD_NAME_52);
		
		PipeWriter.publishWrites(pipe);
		
				
		assertTrue(PipeReader.tryReadFragment(pipe));
		
		assertEquals("name",PipeReader.readUTF8(pipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_NAME_52, new StringBuilder()).toString());
		assertEquals("en",PipeReader.readUTF8(pipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_LANGUAGE_60, new StringBuilder()).toString());
		assertEquals("this is todays date",PipeReader.readUTF8(pipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_CREATEDAT_57, new StringBuilder()).toString());
		
		PipeReader.releaseReadLock(pipe);
		
		assertTrue(PipeReader.tryReadFragment(pipe));
		
		assertEquals("2 name",PipeReader.readUTF8(pipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_NAME_52, new StringBuilder()).toString());
		assertEquals("2 en",PipeReader.readUTF8(pipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_LANGUAGE_60, new StringBuilder()).toString());
		assertEquals("2 this is todays date",PipeReader.readUTF8(pipe, TwitterEventSchema.MSG_USERPOST_101_FIELD_CREATEDAT_57, new StringBuilder()).toString());
		
		PipeReader.releaseReadLock(pipe);
		
	}
	
	
}
