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
		Pipe<RawDataSchema> blob = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		Pipe<RawDataSchema> slab = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		blob.initBuffers();
		slab.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writerBlob = new DataOutputBlobWriter<RawDataSchema>(blob);
		DataOutputBlobWriter<RawDataSchema> writerSlab = new DataOutputBlobWriter<RawDataSchema>(slab);
		
		//encode a string on blolb using the static method
		PhastEncoder.encodeString(writerSlab, writerBlob,  "This is a test");
		
		writerBlob.close();
		writerSlab.close();
		
		//check what is on the pipe
		DataInputBlobReader<RawDataSchema> readerBlob = new DataInputBlobReader<RawDataSchema>(blob);
		DataInputBlobReader<RawDataSchema> readerSlab = new DataInputBlobReader<RawDataSchema>(slab);
		//should be -63
		int test = readerSlab.readPackedInt();
		//char length is 14 so this should be 28
		int lengthOfString = readerSlab.readPackedInt();
		//the string
		StringBuilder value = new StringBuilder();
		readerBlob.readPackedChars(value);
		
		readerBlob.close();
		readerSlab.close();
		
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
	
	
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////                    MASTER LONG ENCODE TEST INCLUDES ALL LONG TESTS                    //////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Test
	public void encodeLongTest() throws IOException{
		//create slab to test
		Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		encodedValuesToValidate.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(encodedValuesToValidate);
		
		//set up dictionaries
		long[] defaultLongDictionary = new long[5];
		defaultLongDictionary[2] = 3468;
		long[] longDictionary = new long[5];
		longDictionary[4] = 2834;
		
		
		long defaultTest = 455;
		
		//should encode: 455
		PhastEncoder.encodeLongPresent(writer, 1, 1, defaultTest);
		//should encode: 2834
		PhastEncoder.incrementLong(longDictionary, writer, 0, 1, 4);
		//should encode: 2835
		PhastEncoder.incrementLong(longDictionary, writer, 1, 1, 4);
		//should encode: 2835
		PhastEncoder.copyLong(longDictionary, writer, 1, 1, 4);
		//should encode: 3468
		PhastEncoder.encodeDefaultLong(defaultLongDictionary, writer, 1, 1, 2, defaultTest);
		//should encode 455
		PhastEncoder.encodeDefaultLong(defaultLongDictionary, writer, 0, 1, 2, defaultTest);
		
		writer.close();
		
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(encodedValuesToValidate);
		assertTrue(reader.readPackedLong()==455);
		assertTrue(reader.readPackedLong()==2834);
		assertTrue(reader.readPackedLong()==2835);
		assertTrue(reader.readPackedLong()==3468);
		assertTrue(reader.readPackedLong()==455);
		reader.close();
	}
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////                    MASTER SHORT ENCODE TEST INCLUDES ALL LONG TESTS                    //////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Test
	public void encodeShortTest() throws IOException{
		//create slab to test
		Pipe<RawDataSchema> encodedValuesToValidate = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		encodedValuesToValidate.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writer = new DataOutputBlobWriter<RawDataSchema>(encodedValuesToValidate);
		
		//set up dictionaries
		short[] defaultShortDictionary = new short[5];
		defaultShortDictionary[2] = 8239;
		short[] shortDictionary = new short[5];
		shortDictionary[4] = 347;
		
		
		short defaultTest = 342;
		
		//should encode: 342
		PhastEncoder.encodeShortPresent(writer, 1, 1, defaultTest);
		//should encode: 347
		PhastEncoder.incrementShort(shortDictionary, writer, 0, 1, 4);
		//should encode: 348
		PhastEncoder.incrementShort(shortDictionary, writer, 1, 1, 4);
		//should encode: 348
		PhastEncoder.copyShort(shortDictionary, writer, 1, 1, 4);
		//should encode: 8239
		PhastEncoder.encodeDefaultShort(defaultShortDictionary, writer, 1, 1, 2, defaultTest);
		//should encode 342
		PhastEncoder.encodeDefaultShort(defaultShortDictionary, writer, 0, 1, 2, defaultTest);
		
		writer.close();
		
		DataInputBlobReader<RawDataSchema> reader = new DataInputBlobReader<RawDataSchema>(encodedValuesToValidate);
		assertTrue(reader.readPackedLong()==342);
		assertTrue(reader.readPackedLong()==347);
		assertTrue(reader.readPackedLong()==348);
		assertTrue(reader.readPackedLong()==8239);
		assertTrue(reader.readPackedLong()==342);
		reader.close();
	}
}
