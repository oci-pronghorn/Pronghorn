package com.ociweb.pronghorn.stage.blocking;

import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class BlockingSupportStage<T extends MessageSchema<T>, P extends MessageSchema<P>, Q extends MessageSchema<Q>> extends PronghornStage {

	private final Pipe<T> input;
	private final Pipe<P> output;
	private final Pipe<Q> timeout;
	private Choosable<T> chooser;
	private Blockable<T, P, Q>[] blockables;
	private Thread[] threads;
	private long[] times;
	private boolean[] needsWorkWaiting;
	private boolean[] completedWorkWaiting;
	private long timeoutNS;
	private boolean isShuttingDown;
	
	public BlockingSupportStage(GraphManager graphManager, Pipe<T> input, Pipe<P> output, Pipe<Q> timeout, long timeoutNS, Choosable<T> chooser, Blockable<T,P,Q> ... blockables) {
		super(graphManager, input, output==timeout ? join(output) : join(output,timeout));
		this.input = input;
		this.output = output;
		this.timeout = timeout;
		this.chooser = chooser;
		this.blockables = blockables;
		this.timeoutNS = timeoutNS;
	}

	@Override
	public void startup() {
		times = new long[blockables.length];
		needsWorkWaiting = new boolean[blockables.length];
		completedWorkWaiting = new boolean[blockables.length];
		
		threads = new Thread[blockables.length];
		ThreadGroup threadGroup = new ThreadGroup("BlockingGroup-"+stageId);
		int t = threads.length;
		while (--t >= 0) {
			Blockable<T, P, Q> blockable = blockables[t];
			threads[t] = new Thread(threadGroup,buildRunnable(t),blockable.name()+"-"+t,blockable.requestedStackSize());
		}
	}
	
	private Runnable buildRunnable(final int instance) {
		
		final Blockable<T, P, Q> b = blockables[instance];

		return new Runnable() {

			@Override
			public void run() {
				synchronized(b) {
					while (!isShuttingDown) {
					
						try {					
							needsWorkWaiting[instance] = true;
							wait();
							needsWorkWaiting[instance] = false;
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							return;//exit now...
						}
												
						try {					
							//needed for external timeout checking
							times[instance] = System.nanoTime();						
							b.run();
							times[instance] = 0;//clear
							
							completedWorkWaiting[instance] = true;
							wait();
							completedWorkWaiting[instance] = false;
							
						} catch (InterruptedException ie) {
							Thread.currentThread().interrupt();
							b.timeout(timeout);
						}					
					}
				}				
			}			
		};
	}

	@Override
	public void run() {
		
		//pick up as much new work as we can
		while (Pipe.hasContentToRead(input)) {
			int choice = chooser.choose(input);
			if (choice>=0 && completedWorkWaiting[choice]) {
				Blockable<T,P,Q> b = blockables[choice];
				synchronized(b) {
					b.begin(input);
					b.notify();					
				}
			} else {
				break;
			}
		}
		
		//check for timeouts
		long now = System.nanoTime();
		int t = times.length;
		while (--t>=0) {
			long duration = now - times[t];
			if (duration>timeoutNS && Pipe.hasRoomForWrite(timeout)) {
				threads[t].interrupt();
			}		
		}	
		
		//finish any complete jobs
		int j = needsWorkWaiting.length;
		while (--j>0) {
			if (needsWorkWaiting[j] && Pipe.hasRoomForWrite(output)) {
				Blockable<T,P,Q> b = blockables[j];
				synchronized(b) {
					b.finish(output);
					b.notify();
				}
			}			
		}
	}

	@Override
	public void shutdown() {
		isShuttingDown = true;
		int t = times.length;
		while (--t>=0) {
			Blockable<T,P,Q> b = blockables[t];
			synchronized(b) {
				threads[t].interrupt();
				b.notify();
			}	
		}
	}
	
}
