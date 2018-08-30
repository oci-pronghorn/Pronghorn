package com.ociweb.pronghorn.stage.blocking;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;

public abstract class Blockable<T extends MessageSchema<T>, P extends MessageSchema<P>, Q extends MessageSchema<Q>> {

	/**
	 * 
	 * @param input Pipe of data to consume
	 * @return true if the work was accepted
	 */
	public abstract boolean begin(Pipe<T> input);
	
	public abstract void run() throws InterruptedException;
	
	public abstract void finish(Pipe<P> output);
	public abstract void timeout(Pipe<Q> output);

	public String name() { //override this to add a custom name to this blockable
		return "BlockingTask"; 
	}

	public int requestedStackSize() {
		return 0; //zero is ignored so the default stack will be used.
	}

}
