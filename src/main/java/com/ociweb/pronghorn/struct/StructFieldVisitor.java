package com.ociweb.pronghorn.struct;

import com.ociweb.pronghorn.pipe.ChannelReader;

public interface StructFieldVisitor<T> {

	public void read(T value, ChannelReader reader);
	
}
