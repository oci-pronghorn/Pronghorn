package com.ociweb.pronghorn.stage.blocking;

import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.ChannelWriter;

public interface BlockingWorkerOperation<M> { 
	
	public boolean execute(ChannelReader input, M source, ChannelWriter output);
	
}
