package com.ociweb.pronghorn.stage.phast;
import static org.junit.Assert.*;

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


public class PhastDecoderTest {
	//string test
	@Test
	public void decodeStringTest() throws IOException{
		Pipe<RawDataSchema> pipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		pipe.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(pipe);
		
		DataOutputBlobWriter.writePackedInt(writer, -63);
		writer.writeUTF("This is a test");
		
		writer.close();
		
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(pipe);
		
		String stest = PhastDecoder.decodeString(reader, false);
		
		reader.close();
		
		assertTrue(stest.compareTo("This is a test") == 0);
		
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////                    MASTER INT DECODE TEST INCLUDES ALL INT TESTS                    ////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Test
	public void decodeIntTests() throws IOException{
		Pipe<RawDataSchema> pipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		pipe.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(pipe);
		
		//writing testing ints
		DataOutputBlobWriter.writePackedInt(writer, 3894);
		DataOutputBlobWriter.writePackedInt(writer, 905);
		DataOutputBlobWriter.writePackedInt(writer, 903);
		DataOutputBlobWriter.writePackedInt(writer, 404);
		
		writer.close();
		
		int[] intDictionary = new int[5];
		intDictionary[0] = 3;
		intDictionary[1] = 56;
		intDictionary[2] = 70;
		
		int[] defaultValues = new int[5];
		defaultValues[0] = 5;
		defaultValues[1] = 60;
		defaultValues[2] = 16;
		
		
		//present int test
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(pipe);
		int test = PhastDecoder.decodePresentInt(reader, 0, 0, false);
		assertTrue(test==3894);
		
		//delta int test
		//test = PhastDecoder.decodeDeltaInt(intDictionary, reader, 0, 1, 0, 10);
		//int test2 = PhastDecoder.decodeDeltaInt(intDictionary, reader, 1, 1, 1, 0);
		//assertTrue(test==961 && test2==961);
		
		//increment test
		test = PhastDecoder.decodeIncrementInt(intDictionary, 0, 0, 0, false);
		//test2 = PhastDecoder.decodeIncrementInt(intDictionary, 1, 0, 1);
		//assertTrue(test == 4 && test2 == 4);
		
		//copy int test
		test = PhastDecoder.decodeCopyInt(intDictionary, reader, 0, 2, 0, false);
		//test2 = PhastDecoder.decodeCopyInt(intDictionary, reader, 1, 2, 1);
		//assertTrue(test == 70 && test2 == 903);
		
		//default int test
		test = PhastDecoder.decodeDefaultInt(reader, 0, defaultValues, 0, 2, false);
		//test2 = PhastDecoder.decodeDefaultInt(reader, 1, defaultValues, 1, 2);
		//assertTrue(test == 16 && test2==404);
		
	}
	
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////                    MASTER LONG DECODE TEST INCLUDES ALL LONG TESTS                  ////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Test
	public void decodeLongTests() throws IOException{
		Pipe<RawDataSchema> pipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		pipe.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(pipe);
		
		//writing testing longs
		DataOutputBlobWriter.writePackedLong(writer, 3894);
		DataOutputBlobWriter.writePackedLong(writer, 905);
		DataOutputBlobWriter.writePackedLong(writer, 903);
		DataOutputBlobWriter.writePackedLong(writer, 404);
		
		writer.close();
		
		long[] longDictionary = new long[5];
		longDictionary[0] = 3;
		longDictionary[1] = 56;
		longDictionary[2] = 70;
		
		long[] defaultValues = new long[5];
		defaultValues[0] = 5;
		defaultValues[1] = 60;
		defaultValues[2] = 16;
		
		
		//present long test
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(pipe);
		long test = PhastDecoder.decodePresentLong(reader, 0, 0, false);
		assertTrue(test==3894);
		
		//delta long test
		test = PhastDecoder.decodeDeltaLong(longDictionary, reader, 0, 1, 0, false);
		long test2 = PhastDecoder.decodeDeltaLong(longDictionary, reader, 1, 1, 1, false);
		assertTrue(test==961 && test2==961);
		
		//increment test
		test = PhastDecoder.decodeIncrementLong(longDictionary, 0, 0, 0, false);
		test2 = PhastDecoder.decodeIncrementLong(longDictionary, 1, 0, 1, false);
		assertTrue(test == 4 && test2 == 4);
		
		//copy long test
		test = PhastDecoder.decodeCopyLong(longDictionary, reader, 0, 2, 0, false);
		test2 = PhastDecoder.decodeCopyLong(longDictionary, reader, 1, 2, 1, false);
		assertTrue(test == 70 && test2 == 903);
		
		//default long test
		test = PhastDecoder.decodeDefaultLong(reader, 0, defaultValues, 0, 2, false);
		test2 = PhastDecoder.decodeDefaultLong(reader, 1, defaultValues, 1, 2, false);
		assertTrue(test == 16 && test2==404);
		
	}
	
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////                  MASTER LONG DECODE TEST INCLUDES ALL SHORT TESTS                   ////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Test
	public void decodeShortTests() throws IOException{
		Pipe<RawDataSchema> pipe = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		pipe.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(pipe);
		
		//writing testing longs
		writer.writePackedShort((short)3894);
		writer.writePackedShort((short)905);
		writer.writePackedShort((short)903);
		writer.writePackedShort((short)404);
		
		writer.close();
		
		short[] shortDictionary = new short[5];
		shortDictionary[0] = 3;
		shortDictionary[1] = 56;
		shortDictionary[2] = 70;
		
		short[] defaultValues = new short[5];
		defaultValues[0] = 5;
		defaultValues[1] = 60;
		defaultValues[2] = 16;
		
		
		//present long test
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(pipe);
		short test = PhastDecoder.decodePresentShort(reader, 0, 0, false);
		assertTrue(test==3894);
		
		//delta long test
		test = PhastDecoder.decodeDeltaShort(shortDictionary, reader, 0, 1, 0, false);
		long test2 = PhastDecoder.decodeDeltaShort(shortDictionary, reader, 1, 1, 1, false);
		assertTrue(test==961 && test2==961);
		
		//increment test
		test = PhastDecoder.decodeIncrementShort(shortDictionary, 0, 0, 0, false);
		test2 = PhastDecoder.decodeIncrementShort(shortDictionary, 1, 0, 1, false);
		assertTrue(test == 4 && test2 == 4);
		
		//copy long test
		test = PhastDecoder.decodeCopyShort(shortDictionary, reader, 0, 2, 0, false);
		test2 = PhastDecoder.decodeCopyShort(shortDictionary, reader, 1, 2, 1, false);
		assertTrue(test == 70 && test2 == 903);
		
		//default long test
		test = PhastDecoder.decodeDefaultShort(reader, 0, defaultValues, 0, 2, false);
		test2 = PhastDecoder.decodeDefaultShort(reader, 1, defaultValues, 1, 2, false);
		assertTrue(test == 16 && test2==404);
		
	}
}
