package com.ociweb.pronghorn.network.http;

import com.ociweb.pronghorn.pipe.util.hash.LongHashTable;

public class SequenceValidator {

	private	LongHashTable sequenceCheck = new LongHashTable(16);
	
	public boolean isValidSequence(long connection, long sequenceCode) {
		int sequenceNo = (int)sequenceCode&Integer.MAX_VALUE;
		boolean result;
		if (!LongHashTable.hasItem(sequenceCheck, connection)) {
			if (!LongHashTable.setItem(sequenceCheck, connection, sequenceNo)) {
				throw new RuntimeException("must grow");
			};
			result = 0==sequenceNo;//first entry must be zero
		} else {
			int lastSeq = LongHashTable.getItem(sequenceCheck, connection);			
			//NOTE: sequence can stay the same or move forward or reset to zero
			result = (lastSeq+1 == sequenceNo) || (lastSeq == sequenceNo) || (0 == sequenceNo);
			if (!result) {
				new Exception("con:"+connection+" last: "+lastSeq+" got "+sequenceNo).printStackTrace();				
			}
			LongHashTable.replaceItem(sequenceCheck, connection, sequenceNo);
			
		}		
		return result;
	}
	
	
}
