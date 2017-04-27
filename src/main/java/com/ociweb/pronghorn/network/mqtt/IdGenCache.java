package com.ociweb.pronghorn.network.mqtt;

public class IdGenCache {

	public int nextFreePacketId = -1;
	public int nextFreePacketIdLimit = -1;
	
	public static int nextPacketId(IdGenCache genCache) {
		return genCache.nextFreePacketId++;
	}
	public static boolean isEmpty(IdGenCache genCache) {
		return genCache.nextFreePacketId >= genCache.nextFreePacketIdLimit;
	}
	
}
