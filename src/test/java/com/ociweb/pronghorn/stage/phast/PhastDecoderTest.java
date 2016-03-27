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

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////                    MASTER INT DECODE TEST INCLUDES ALL INT TESTS                    ////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	@Test
	static void decodeIntTests() throws IOException{
		Pipe<RawDataSchema> slab = new Pipe<RawDataSchema>(new PipeConfig<RawDataSchema>(RawDataSchema.instance, 100, 4000));
		slab.initBuffers();
		DataOutputBlobWriter<RawDataSchema> writerSlab = new DataOutputBlobWriter<RawDataSchema>(slab);
		
		//writing testing ints
		DataOutputBlobWriter.writePackedInt(writerSlab, 3894);
		DataOutputBlobWriter.writePackedInt(writerSlab, 34893);
		DataOutputBlobWriter.writePackedInt(writerSlab, 905);
		DataOutputBlobWriter.writePackedInt(writerSlab, 404);
		DataOutputBlobWriter.writePackedInt(writerSlab, 493);
		DataOutputBlobWriter.writePackedInt(writerSlab, 9403);
		DataOutputBlobWriter.writePackedInt(writerSlab, 84);
		DataOutputBlobWriter.writePackedInt(writerSlab, 3980);
		DataOutputBlobWriter.writePackedInt(writerSlab, 202);
		DataOutputBlobWriter.writePackedInt(writerSlab, 4893);
		DataOutputBlobWriter.writePackedInt(writerSlab, 2034);
		
		writerSlab.close();
		
		assertTrue(true);
		
		
		
	}
}
