package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

public class SchemalessPipeTest {

	private final SchemalessFixedFieldPipeConfig config = new SchemalessFixedFieldPipeConfig(16);
	
	@Test
	public void schemalessIntTest() {
			
				
		Pipe<MessageSchemaDynamic> p = new Pipe<MessageSchemaDynamic>(config);
		p.setPublishBatchSize(p, 0); //all publishes happen immediately
		p.setReleaseBatchSize(p, 0); //all releases happen immediately
		p.initBuffers();
		
				
		Random rSource = new Random(123);		
		Random rConfirm = new Random(123);		
		
		int i = 33;//test size simple round trip;
		while (--i>=0) {
		
			Pipe.addIntValue(rSource.nextInt(), p);		
			Pipe.publishWritesBatched(p);
					
			assertTrue(Pipe.contentRemaining(p)==1);
			
			int readValue =	Pipe.takeInt(p);		
	        Pipe.releaseReadsBatched(p);
	        
	        assertEquals(rConfirm.nextInt(), readValue);
			assertTrue(Pipe.contentRemaining(p)==0);
        
			//test full fill of pipe and empty		
			while (Pipe.contentRemaining(p)<p.sizeOfSlabRing) {
				Pipe.addIntValue(rSource.nextInt(), p);		
				Pipe.publishWritesBatched(p);
			}
			
			while (Pipe.contentRemaining(p)>0) {
				int readValueX =	Pipe.takeInt(p);	
				assertEquals(rConfirm.nextInt(), readValueX);
				Pipe.releaseReadsBatched(p);
			}
		}
	}
	
	@Test
	public void schemalessLongTest() {
				
		
		Pipe<MessageSchemaDynamic> p = new Pipe<MessageSchemaDynamic>(config);
		p.setPublishBatchSize(p, 0); //all publishes happen immediately
		p.setReleaseBatchSize(p, 0); //all releases happen immediately
		p.initBuffers();
		
		Random rSource = new Random(123);		
		Random rConfirm = new Random(123);		
		
		int i = 33;//test size simple round trip;
		while (--i>=0) {
		
			Pipe.addLongValue(rSource.nextLong(), p);		
			Pipe.publishWritesBatched(p);
					
			assertTrue(Pipe.contentRemaining(p)==2);
			
			long readValue =	Pipe.takeLong(p);		
	        Pipe.releaseReadsBatched(p);
	        
	        assertEquals(rConfirm.nextLong(), readValue);
			assertTrue(Pipe.contentRemaining(p)==0);
        
			//test full fill of pipe and empty		
			while (Pipe.contentRemaining(p)<p.sizeOfSlabRing) {
				Pipe.addLongValue(rSource.nextLong(), p);		
				Pipe.publishWritesBatched(p);
			}
			
			while (Pipe.contentRemaining(p)>0) {
				long readValueX =	Pipe.takeLong(p);	
				assertEquals(rConfirm.nextLong(), readValueX);
				Pipe.releaseReadsBatched(p);
			}
		}
	}
	
	
}
