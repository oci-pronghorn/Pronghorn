package com.ociweb.pronghorn.stage.phast;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

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
	public void testEncodeString() throws IOException{
		//create a new blob pipe to put a string on 
		Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		encodedValuesToValidate.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(encodedValuesToValidate);
		
		//encode a string on there using the static method
		PhastEncoder.encodeString(writer, "This is a test");
		Pipe.publishAllBatchedWrites(encodedValuesToValidate);
		
		//check what is on the pipe
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(encodedValuesToValidate);
		//should be -63
		int test = reader.readPackedInt();
		//char length is 14 so this should be 28
		int lengthOfString = reader.readPackedInt();
		//the string
		StringBuilder value = new StringBuilder();
		reader.readPackedChars(value);
		
		reader.close();
		
		String s = value.toString();
		assertTrue((test==-63) && (lengthOfString==28) && (s.compareTo("This is a test")==0));
		
	}
	
	@Test
	public void testIncrementInt(){
		//create blob for test
		Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		encodedValuesToValidate.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(encodedValuesToValidate);
		
		//create int dictionary
		int[] intDictionary = new int[5];
		Arrays.fill(intDictionary, 0);
		intDictionary[2] = 5;
		
		//make it increment 2 values 0 and 5
		PhastEncoder.incrementInt(intDictionary, writer, 0, 0, 2);
		PhastEncoder.incrementInt(intDictionary, writer, 0, 0, 1);
		
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(encodedValuesToValidate);
		//should be 6
		int test1 = reader.readPackedInt();
		//should be 1
		int test2 = reader.readPackedInt();
		
		assertTrue((test1 == 6) && (test2==1));
	}
}
