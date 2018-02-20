package com.ociweb.pronghorn.util.parse;

import java.io.PrintStream;

import com.ociweb.pronghorn.pipe.ChannelReader;

public interface JSONReader {

	long getLong(byte[] field, ChannelReader reader);
	<A extends Appendable> A getText(byte[] field, ChannelReader reader, A target);
	boolean getBoolean(byte[] field, ChannelReader reader);
	boolean wasAbsent(ChannelReader reader);
	
	long getDecimalMantissa(byte[] field, ChannelReader reader);
	byte getDecimalPosition(byte[] field, ChannelReader reader);
	<A extends Appendable> A dump(ChannelReader reader, A out);
	
	
	
}
