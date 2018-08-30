package com.ociweb.pronghorn.stage.blocking;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;

public interface UnchosenMessage<T extends MessageSchema<T>> {

		boolean message(Pipe<T> pipe);
}
