package com.ociweb.pronghorn.ring;

import com.ociweb.pronghorn.ring.RingBuffer;

public interface RingWraith extends Runnable {

	public void setInputRing(RingBuffer input);
	public void setOutputRing(RingBuffer output);
	
}
