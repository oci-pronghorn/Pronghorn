package com.ociweb.pronghorn.ring;

import com.ociweb.pronghorn.ring.RingBuffer;

@Deprecated
public interface RingWraith extends Runnable {
	@Deprecated
	public void setInputRing(RingBuffer input);
	@Deprecated
	public void setOutputRing(RingBuffer output);
	
}
