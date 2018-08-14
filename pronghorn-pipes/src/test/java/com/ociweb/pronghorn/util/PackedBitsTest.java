package com.ociweb.pronghorn.util;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;

public class PackedBitsTest {

	@Test
	public void simpleTest() {
		
		PackedBits pb = new PackedBits();
		
		pb.setValue(0,  1);//  1
		pb.setValue(2,  1);//  4
		pb.setValue(7,  1);//128
		pb.setValue(21, 0);
		
		Pipe<RawDataSchema> p = RawDataSchema.instance.newPipe(4, 40);
		p.initBuffers();
		
		Pipe.addMsgIdx(p, 0);
		DataOutputBlobWriter<RawDataSchema> out = Pipe.openOutputStream(p);
				
		try {
			pb.write(out);
		} catch (IOException e) {
			e.printStackTrace();
		}
		DataOutputBlobWriter.closeLowLevelField(out);
		Pipe.confirmLowLevelWrite(p);
		Pipe.publishWrites(p);
		
		Pipe.takeMsgIdx(p);
		DataInputBlobReader<RawDataSchema> in = Pipe.openInputStream(p);
		
		long value = in.readPackedLong();
		assertEquals(133, value);
	
		
	}
	
	
}
