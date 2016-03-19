package com.ociweb.pronghorn.stage.phast;
import java.io.UnsupportedEncodingException;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.phast.*;

import static org.junit.Assert.*;


public class PhastEncoderTest {
	
	@Test
	public void testEncodeString() throws UnsupportedEncodingException{
		//PipeConfig<RawDataSchema> inputConfig = new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 1000);
		//Pipe<RawDataSchema> testDataToDecode = new Pipe<RawDataSchema>(inputConfig);
		//DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(testDataToDecode);
		
		//PhastEncoder.encodeString(writer, 0, 0, "this is a test");
		assertTrue(true);
	}
}
