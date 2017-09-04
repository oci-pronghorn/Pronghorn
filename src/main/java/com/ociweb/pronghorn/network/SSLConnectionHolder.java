package com.ociweb.pronghorn.network;

public abstract class SSLConnectionHolder {
	
	public abstract SSLConnection connectionForSessionId(long hostId);
	
}
