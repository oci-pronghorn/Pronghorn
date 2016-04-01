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
		Pipe<RawDataSchema> slab = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		Pipe<RawDataSchema> blob = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		slab.initBuffers();
		blob.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writerSlab = new DataOutputBlobWriter<RawDataSchema>(slab);
		DataOutputBlobWriter<RawDataSchema> writerBlob = new DataOutputBlobWriter<RawDataSchema>(blob);
		
		DataOutputBlobWriter.writePackedInt(writerSlab, -63);
		DataOutputBlobWriter.writePackedInt(writerSlab, 28);
		DataOutputBlobWriter.writePackedChars(writerBlob, "This is a test");
		
		writerSlab.close();
		writerBlob.close();
		
		DataInputBlobReader<RawDataSchema> readerSlab = new DataInputBlobReader<RawDataSchema>(slab);
		DataInputBlobReader<RawDataSchema> readerBlob = new DataInputBlobReader<RawDataSchema>(blob);
		
		String stest = PhastDecoder.decodeString(readerSlab, readerBlob);
		
		assertTrue(stest.compareTo("This is a test") == 0);
		
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////                    MASTER INT DECODE TEST INCLUDES ALL INT TESTS                    ////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Test
	public void decodeIntTests() throws IOException{
		Pipe<RawDataSchema> slab = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		slab.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writerSlab = new DataOutputBlobWriter<RawDataSchema>(slab);
		
		//writing testing ints
		DataOutputBlobWriter.writePackedInt(writerSlab, 3894);
		DataOutputBlobWriter.writePackedInt(writerSlab, 905);
		DataOutputBlobWriter.writePackedInt(writerSlab, 903);
		DataOutputBlobWriter.writePackedInt(writerSlab, 404);
		
		writerSlab.close();
		
		int[] intDictionary = new int[5];
		intDictionary[0] = 3;
		intDictionary[1] = 56;
		intDictionary[2] = 70;
		
		int[] defaultValues = new int[5];
		defaultValues[0] = 5;
		defaultValues[1] = 60;
		defaultValues[2] = 16;
		
		
		//present int test
		DataInputBlobReader<RawDataSchema> readerSlab = new DataInputBlobReader<RawDataSchema>(slab);
		int test = PhastDecoder.decodePresentInt(readerSlab, 0, 0);
		assertTrue(test==3894);
		
		//delta int test
		test = PhastDecoder.decodeDeltaInt(intDictionary, readerSlab, 0, 1, 0, 10);
		int test2 = PhastDecoder.decodeDeltaInt(intDictionary, readerSlab, 1, 1, 1, 0);
		assertTrue(test==961 && test2==961);
		
		//increment test
		test = PhastDecoder.decodeIncrementInt(intDictionary, 0, 0, 0);
		test2 = PhastDecoder.decodeIncrementInt(intDictionary, 1, 0, 1);
		assertTrue(test == 4 && test2 == 4);
		
		//copy int test
		test = PhastDecoder.decodeCopyInt(intDictionary, readerSlab, 0, 2, 0);
		test2 = PhastDecoder.decodeCopyInt(intDictionary, readerSlab, 1, 2, 1);
		assertTrue(test == 70 && test2 == 903);
		
		//default int test
		test = PhastDecoder.decodeDefaultInt(readerSlab, 0, defaultValues, 0, 2);
		test2 = PhastDecoder.decodeDefaultInt(readerSlab, 1, defaultValues, 1, 2);
		assertTrue(test == 16 && test2==404);
		
	}
	
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////                    MASTER LONG DECODE TEST INCLUDES ALL LONG TESTS                  ////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Test
	public void decodeLongTests() throws IOException{
		Pipe<RawDataSchema> slab = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		slab.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writerSlab = new DataOutputBlobWriter<RawDataSchema>(slab);
		
		//writing testing longs
		DataOutputBlobWriter.writePackedLong(writerSlab, 3894);
		DataOutputBlobWriter.writePackedLong(writerSlab, 905);
		DataOutputBlobWriter.writePackedLong(writerSlab, 903);
		DataOutputBlobWriter.writePackedLong(writerSlab, 404);
		
		writerSlab.close();
		
		long[] longDictionary = new long[5];
		longDictionary[0] = 3;
		longDictionary[1] = 56;
		longDictionary[2] = 70;
		
		long[] defaultValues = new long[5];
		defaultValues[0] = 5;
		defaultValues[1] = 60;
		defaultValues[2] = 16;
		
		
		//present long test
		DataInputBlobReader<RawDataSchema> readerSlab = new DataInputBlobReader<RawDataSchema>(slab);
		long test = PhastDecoder.decodePresentLong(readerSlab, 0, 0);
		assertTrue(test==3894);
		
		//delta long test
		test = PhastDecoder.decodeDeltaLong(longDictionary, readerSlab, 0, 1, 0);
		long test2 = PhastDecoder.decodeDeltaLong(longDictionary, readerSlab, 1, 1, 1);
		assertTrue(test==961 && test2==961);
		
		//increment test
		test = PhastDecoder.decodeIncrementLong(longDictionary, 0, 0, 0);
		test2 = PhastDecoder.decodeIncrementLong(longDictionary, 1, 0, 1);
		assertTrue(test == 4 && test2 == 4);
		
		//copy long test
		test = PhastDecoder.decodeCopyLong(longDictionary, readerSlab, 0, 2, 0);
		test2 = PhastDecoder.decodeCopyLong(longDictionary, readerSlab, 1, 2, 1);
		assertTrue(test == 70 && test2 == 903);
		
		//default long test
		test = PhastDecoder.decodeDefaultLong(readerSlab, 0, defaultValues, 0, 2);
		test2 = PhastDecoder.decodeDefaultLong(readerSlab, 1, defaultValues, 1, 2);
		assertTrue(test == 16 && test2==404);
		
	}
}
