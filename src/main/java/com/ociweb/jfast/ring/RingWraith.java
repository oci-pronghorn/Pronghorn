package com.ociweb.jfast.ring;

import com.ociweb.jfast.ring.RingBuffer;

public interface RingWraith extends Runnable {

	public void setInputRing(RingBuffer input);
	public void setOutputRing(RingBuffer output);
	
}
