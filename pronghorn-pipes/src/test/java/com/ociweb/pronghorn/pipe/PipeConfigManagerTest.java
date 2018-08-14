package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.*;

import org.junit.Test;

public class PipeConfigManagerTest {

	@Test
	public void test() {
		
		
		PipeConfigManager pcm = new PipeConfigManager();
		
		pcm.addConfig(RawDataSchema.instance.newPipeConfig(10, 500));
		
		PipeConfig<RawDataSchema> first = pcm.getConfig(RawDataSchema.class);

		assertTrue(first.minimumFragmentsOnPipe()>=10);
		assertTrue(first.minimumFragmentsOnPipe()<=20);
		assertTrue(first.maxVarLenSize()>= 500);
		assertTrue(first.maxVarLenSize()<=1000);
		
				
		pcm.addConfig(RawDataSchema.instance.newPipeConfig(20, 700));
		
		PipeConfig<RawDataSchema> second = pcm.getConfig(RawDataSchema.class);

		assertTrue(second.minimumFragmentsOnPipe()>=20);
		assertTrue(second.minimumFragmentsOnPipe()<=40);
		assertTrue(second.maxVarLenSize()>= 700);
		assertTrue(second.maxVarLenSize()<=1400);	
		
	}
	
	
}
