package com.ociweb.pronghorn.struct;

import com.ociweb.pronghorn.pipe.ChannelReader;

public interface BStructFieldVisitor<T> {

	public void read(T value, ChannelReader reader);
	
}
