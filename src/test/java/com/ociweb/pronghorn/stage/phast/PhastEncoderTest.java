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
		writer.close();
		
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
	public void testIncrementInt() throws IOException{
		//create blob for test
		Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		encodedValuesToValidate.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(encodedValuesToValidate);
		
		//create int dictionary
		int[] intDictionary = new int[5];
		Arrays.fill(intDictionary, 0);
		intDictionary[2] = 5;
		intDictionary[1] = 5;
		
		//increment one and not the other
		PhastEncoder.incrementInt(intDictionary, writer, 0, 1, 2);
		PhastEncoder.incrementInt(intDictionary, writer, 1, 1, 1);
		
		writer.close();
		
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(encodedValuesToValidate);
		//should be 5
		int test1 = reader.readPackedInt();
		//should be 6
		int test2 = reader.readPackedInt();
		reader.close();
		
		assertTrue((test1 == 5) && (test2==6));
	}
	
	@Test
	public void copyIntTest() throws IOException{
		//create blob for test
		Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		encodedValuesToValidate.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(encodedValuesToValidate);
		
		//create int dictionary
		int[] intDictionary = new int[5];
		Arrays.fill(intDictionary, 0);
		intDictionary[2] = 5;
		
		//make it increment 2 values 0 and 5
		PhastEncoder.copyInt(intDictionary, writer, 0, 0, 2);
		writer.close();
		
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(encodedValuesToValidate);
		//should be 5
		int test1 = reader.readPackedInt();
		reader.close();
		assertTrue(test1 == 5);
	}
	
	@Test
	public void defaultIntTest() throws IOException{
		//create a blob to test
		Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		encodedValuesToValidate.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(encodedValuesToValidate);
		
		//make int array
		int[] defaultInt = new int[5];
		defaultInt[3] = 4;
		
		//should encode 16
		PhastEncoder.encodeDefaultInt(defaultInt, writer, 1, 1, 3, 16);
		//should encode 4
		PhastEncoder.encodeDefaultInt(defaultInt, writer, 0, 1, 3, 16);
		
		writer.close();
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(encodedValuesToValidate);
		int test1 = reader.readPackedInt();
		int test2 = reader.readPackedInt();
		reader.close();
		
		assertTrue(test1==16 && test2==4);
	}
	
	@Test
	public void ecnodePresentLongTest() throws IOException{
		//create a blob to test
		Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		encodedValuesToValidate.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(encodedValuesToValidate);
		
		//encode long
		PhastEncoder.encodeLongPresent(writer, 1, 1, 5714);
		writer.close();
		
		//check it
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(encodedValuesToValidate);
		long test = reader.readPackedLong();
		assertTrue(test == 5714);
	}
}
