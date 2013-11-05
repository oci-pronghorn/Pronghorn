package com.ociweb.jfast.primitive;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;

import org.junit.Test;

import com.ociweb.jfast.primitive.adapter.FASTInputStream;

public class PrimitivePMAPTest {

	private static final int bufferSize = 4096;
	
	@Test
	public void testReaderSingle() {
		
		byte[] testData = new byte[] {((byte)Integer.valueOf("00111010", 2).intValue()),
									    ((byte)Integer.valueOf("10001011", 2).intValue())};
		
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(testData));
		PrimitiveReader pr = new PrimitiveReader(bufferSize, input);
		
		int maxPMapSize = testData.length; //in bytes
		//open this pmap
		pr.readPMap(maxPMapSize);
		
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		//next byte
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		//unwritten and assumed trailing zeros test
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		//close
		pr.popPMap();
		
	}
	
	@Test
	public void testReaderNested() {
		
		byte[] testData = new byte[] {((byte)Integer.valueOf("00111010", 2).intValue()),
									    ((byte)Integer.valueOf("10001011", 2).intValue()),
									    //second pmap starts here
									    ((byte)Integer.valueOf("00000000", 2).intValue()),
									    ((byte)Integer.valueOf("11111111", 2).intValue())							    
		};
		
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(testData));
		PrimitiveReader pr = new PrimitiveReader(bufferSize, input);
		
		//open this pmap
		pr.readPMap(2);
		
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		//stop at this point to load another pmap and read it all before continuing
			pr.readPMap(2);
			//first byte of second pmap
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			//second byte of second pmap
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			assertEquals(1,pr.popPMapBit());
			//unwritten and assumed trailing zeros test
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			assertEquals(0,pr.popPMapBit());
			//resume with first pmap
			pr.popPMap();
		///
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		//next byte from first pmap
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(0,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		assertEquals(1,pr.popPMapBit());
		//close
		pr.popPMap();
		
	}
}
