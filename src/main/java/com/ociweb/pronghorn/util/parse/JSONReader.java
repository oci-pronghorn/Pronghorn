package com.ociweb.pronghorn.util.parse;

import com.ociweb.pronghorn.pipe.ChannelReader;

public interface JSONReader {

	long getLong(int fieldId, ChannelReader reader);
	<A extends Appendable> A getText(int fieldId, ChannelReader reader, A target);
	boolean getBoolean(int fieldId, ChannelReader reader);
	boolean wasAbsent(int fieldId, ChannelReader reader);
	
	long getDecimalMantissa(int fieldId, ChannelReader reader);
	byte getDecimalPosition(int fieldId, ChannelReader reader);
	<A extends Appendable> A dump(ChannelReader reader, A out);
	
	void clear();
	
	
}
