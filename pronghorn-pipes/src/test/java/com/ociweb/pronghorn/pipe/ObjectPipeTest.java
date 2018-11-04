package com.ociweb.pronghorn.pipe;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class ObjectPipeTest {
	
	class TestObj {
		public int field;		
	}
	
	@Test
	public void headTailReleaseTest() {
		
		ObjectPipe<TestObj> op = new ObjectPipe(2, TestObj.class, TestObj::new );
		
		///////
		//load data
		///////
		TestObj data1 = op.headObject();
		data1.field = 1;
		op.moveHeadForward();
		
		TestObj data2 = op.headObject();
		data2.field = 2;
		op.moveHeadForward();
		
		TestObj data3 = op.headObject();
		data3.field = 3;
		op.moveHeadForward();
				
		assertEquals(null,op.headObject());//no room since we only have 4 and we keep 1 for write.
		
		//////
		//consume
		//////
		
		TestObj read1 = op.tailObject();
		op.moveTailForward();
		assertEquals(1,read1.field);
		
		TestObj read2 = op.tailObject();
		op.moveTailForward();
		assertEquals(2,read2.field);

		TestObj read3 = op.tailObject();
		op.moveTailForward();
		assertEquals(3,read3.field);
		
		assertEquals(null,op.headObject());//still null since we did not publish the tail
		op.publishTailPosition();
		
		assertNotNull(op.headObject());		
		
	}
	
	
}
