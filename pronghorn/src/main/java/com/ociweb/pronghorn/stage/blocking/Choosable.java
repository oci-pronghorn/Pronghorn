package com.ociweb.pronghorn.stage.blocking;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;

@Deprecated
public interface Choosable<T extends MessageSchema<T>> {

	int choose(Pipe<T> t);
	
}
