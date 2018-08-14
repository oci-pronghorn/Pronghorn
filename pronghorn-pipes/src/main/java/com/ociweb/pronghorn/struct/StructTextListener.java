package com.ociweb.pronghorn.struct;

import com.ociweb.pronghorn.pipe.TextReader;

public interface StructTextListener {

	void value(TextReader reader, boolean isNull, int[] position, int[] size, int instance, int totalCount);
	
}
