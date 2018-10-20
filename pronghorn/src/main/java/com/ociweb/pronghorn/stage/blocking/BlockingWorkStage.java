package com.ociweb.pronghorn.stage.blocking;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class BlockingWorkStage extends PronghornStage {  //move to PH

	private final class BlockingRunnable implements Runnable {
		private final ReentrantLock reentrantLock;
		private final Pipe<RawDataSchema> output;
		private final Pipe<RawDataSchema> input;
		private final BlockingWorker worker;

		private BlockingRunnable(ReentrantLock reentrantLock, Pipe<RawDataSchema> output, Pipe<RawDataSchema> input,
				BlockingWorker worker) {
			this.reentrantLock = reentrantLock;
			this.output = output;
			this.input = input;
			this.worker = worker;
		}

		@Override
		public void run() {			
			    reentrantLock.lock();
			    
			    synchronized(worker) {
					assert(reentrantLock.isLocked());
				
					while (!isShuttingDown.get()) {
					
						//TODO: need to check last run time for timeout support...
						
						Thread.yield();//helps this thread stay working by increasing the odds that we have work
						if (Pipe.hasContentToRead(input) && Pipe.hasRoomForWrite(output)) {
					
							if (Pipe.peekMsg(input, -1)) {
								break;	//leave while and finish thread, on exit it will unlock								
							}
							
							//what if this does not come back while we are shutting down?
							try {
								worker.doWork(input,output);//NOTE: we want to add timeout feature in the near future...
							} catch (Exception e) {
								e.printStackTrace();
								//unknown state so must shutdown.
								isShuttingDown.set(true);
								break;
							}
						} else {
										
					
									//System.out.println("sleep "+System.currentTimeMillis());
									reentrantLock.unlock(); //allow external code to wake me up.
									
									/////////////////////////
									//this block very complex however
									//this is needed for clean shutdown
									/////////////////////////
									boolean exit = false;
									do {
										try {
											worker.wait(); 
										} catch (InterruptedException e) {
											Thread.currentThread().interrupt();
											exit= true;
											break;
										} 
									} while (!reentrantLock.tryLock());//in case the parent stage has grabbed this lock.
									if (exit) {
										break;
									}
									/////////////////////////////
									////////////////////////////
								
						}
					}
					//shutting down now
					reentrantLock.unlock();
				}
				
		}
	}

	private Thread[] threads;	
	private ReentrantLock[] locks;
	private BlockingWorker[] workers;
	private Pipe<RawDataSchema>[] inputs;	

	//all my threads will read this for shutdown
	private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
	
	public BlockingWorkStage(GraphManager graphManager, 
			                    Pipe<RawDataSchema>[] inputs, 
			                    Pipe<RawDataSchema>[] outputs,
			                    BlockingWorkerProducer producer 
								) {
		
		super(graphManager, inputs, outputs);		
		assert(inputs.length==outputs.length);
		
		this.inputs = inputs;
		
		ThreadGroup threadGroup = new ThreadGroup("BlockingGroup-"+stageId);
		this.threads = new Thread[inputs.length];
		this.locks = new ReentrantLock[inputs.length];
		this.workers = new BlockingWorker[inputs.length];
		
		long requestedStackSize = 0; //use default ...
		
		int i = inputs.length;
		while (--i >= 0) {
			this.workers[i] = producer.newWorker();
			this.locks[i] = new ReentrantLock();
			Runnable r = new BlockingRunnable(this.locks[i], outputs[i], this.inputs[i], this.workers[i]);
			threads[i] = new Thread(threadGroup,r, producer.name()+"-"+i, requestedStackSize);			
		}		
	}
	
	@Override
	public void startup() {
		int i = inputs.length;
		while (--i >= 0) {
			threads[i].start();					
		}
	}

	
	@Override
	public void run() {
		
		//wake up the workers if needed.
		int i = inputs.length;
		while(--i >= 0) {						
			
			//notify if we have data waiting and it is not already locked and running
			//only check if not locked because otherwise it is already running
			if ((!locks[i].isLocked()) && Pipe.hasContentToRead(inputs[i]) && locks[i].tryLock() ) {
				synchronized(workers[i]) { 
					try {			
						workers[i].notifyAll();
					} finally {
						locks[i].unlock();
					}
				}
			}
		}		
	}

}
