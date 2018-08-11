package com.ociweb.pronghorn.network.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdGenCache {

	private final static Logger logger = LoggerFactory.getLogger(IdGenCache.class);
	
	private int nextFreePacketId = -1;
	private int nextFreePacketIdLimit = -1;
	
	public static int nextPacketId(IdGenCache genCache) {
		return genCache.nextFreePacketId++;
	}
	public static boolean isEmpty(IdGenCache genCache) {
		return genCache.nextFreePacketId >= genCache.nextFreePacketIdLimit;
	}
	public void nextFreeRange(int range) {
		
		//logger.info("nextFree range {}->{} for {}",IdGenStage.rangeBegin(range),IdGenStage.rangeEnd(range),Integer.toHexString(range));
				
		nextFreePacketId = IdGenStage.rangeBegin(range);
		nextFreePacketIdLimit = IdGenStage.rangeEnd(range);

	}
	
}
