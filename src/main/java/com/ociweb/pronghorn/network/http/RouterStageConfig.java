package com.ociweb.pronghorn.network.http;

public interface RouterStageConfig {

	public int registerRoute(CharSequence route, byte[] ... headers);

	public int headerId(byte[] bytes);
	
}
