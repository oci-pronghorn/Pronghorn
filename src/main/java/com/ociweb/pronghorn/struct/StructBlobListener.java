package com.ociweb.pronghorn.struct;

import com.ociweb.pronghorn.pipe.ChannelReader;

public interface StructBlobListener {

	void value(ChannelReader reader, int[] position, int[] size, int instance, int totalCount);
	
}
