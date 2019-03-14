package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SeriesDecimalSchemaTest {

	@Test
	public void simpleTest() {
		
		Pipe<SeriesDecimalSchema> p = SeriesDecimalSchema.instance.newPipe(4); //small for 1<<18 record
		p.initBuffers();
		//from another pipe or source we know that we need to read N
		//so we keep consuming messages until N is reached.
		int[] starts = Pipe.from(p).messageStarts;
		
		
		Pipe.addMsgIdx(p, starts[0] );
		Pipe.addDecimal(0, 1, p);
		Pipe.confirmLowLevelWrite(p, Pipe.sizeOf(p, starts[0]));
		Pipe.publishWrites(p);
		
		assertTrue(	Pipe.hasRoomForWrite(p, Pipe.sizeOf(p, starts[5])) );
		
		Pipe.addMsgIdx(p, starts[1] );
		Pipe.addDecimal(0, 2, p);
		Pipe.addDecimal(0, 2, p);
		Pipe.confirmLowLevelWrite(p, Pipe.sizeOf(p, starts[1]));
		Pipe.publishWrites(p);
		
		int msgIdx;
		int e;
		long m;
		
		msgIdx = Pipe.takeMsgIdx(p);
		assertEquals(starts[0], msgIdx);
		
		e = Pipe.takeInt(p);
		m = Pipe.takeLong(p);
		assertEquals(1,m);
		Pipe.confirmLowLevelRead(p, Pipe.sizeOf(p, starts[0]));
		Pipe.releaseReadLock(p);
		
		msgIdx = Pipe.takeMsgIdx(p);
		assertEquals(starts[1], msgIdx);
		e = Pipe.takeInt(p);
		m = Pipe.takeLong(p);
		assertEquals(2,m);
		e = Pipe.takeInt(p);
		m = Pipe.takeLong(p);
		assertEquals(2,m);
		Pipe.confirmLowLevelRead(p, Pipe.sizeOf(p, starts[1]));
		Pipe.releaseReadLock(p);
		
		
	}
	
}
