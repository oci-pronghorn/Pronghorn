package com.ociweb.pronghorn.network.http;

import com.ociweb.pronghorn.network.config.HTTPSpecification;

public interface RouterStageConfig {

	public int registerRoute(CharSequence route, byte[] ... headers);

	public int headerId(byte[] bytes);

	public HTTPSpecification httpSpec();
	
}
