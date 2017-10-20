package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ociweb.pronghorn.network.schema.NetPayloadSchema;

public class PipeConfigMemTest {
	
	@Test
	public void memoryUsage() {
		
		PipeConfig<NetPayloadSchema> netPayloadConfig = new PipeConfig<NetPayloadSchema>(
				NetPayloadSchema.instance, 10,1<<12);
		
		int blob = 1<<netPayloadConfig.blobBits;
		int slab = 1<<netPayloadConfig.slabBits;
		
		int expectedBlob = 10 * (1<<12);
		//System.err.println(slab+"  "+blob+" expected blob "+expectedBlob);
		
		assertTrue (blob <  expectedBlob*2);//real blob should be no more than 2x larger than expected
		
	}
}
