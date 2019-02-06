package com.ociweb.pronghorn.stage.monitor;

import static com.ociweb.pronghorn.stage.monitor.PipeMonitorSchema.MSG_RINGSTATSAMPLE_100;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Stage which monitors pipes in real time.
 * This data is passed along for the telemetry.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class PipeMonitorStage extends PronghornStage {

	private final Pipe<?>[] observedPipe;
	private final Pipe<PipeMonitorSchema>[] notifyRingBuffer;
	private final GraphManager gm;
	private static final Logger logger = LoggerFactory.getLogger(PipeMonitorStage.class);
	private long dropped = 0;

	/**
	 * This class should be used with the ScheduledThreadPoolExecutor for 
	 * controlling the rate of samples
	 * 
	 * @param observedRingBuffer _out_ observation pipes
	 * @param notifyRingBuffer _out_ notify pipes
	 */
	public PipeMonitorStage(GraphManager gm, 
						    Pipe<?>[] observedRingBuffer, 
						    Pipe<PipeMonitorSchema>[] notifyRingBuffer) {
		
		//the observed ring buffer is NOT an input
		super(gm, NONE, notifyRingBuffer); 
		
		assert(observedRingBuffer.length == notifyRingBuffer.length);
		this.observedPipe = observedRingBuffer;
		this.notifyRingBuffer = notifyRingBuffer;
		
		this.gm = gm;
		this.setNotaFlag(PronghornStage.FLAG_MONITOR);
		
	}
	
	@Override
	public void startup() {
		int i = notifyRingBuffer.length;
		while (--i>=0) {
			Pipe.setPublishBatchSize(notifyRingBuffer[i], 0);//can not be done earlier 	  
		}
	}
	
	@Override
	public void run() {
		int i = notifyRingBuffer.length;
		while (--i>=0) {
			monitorSinglePipe(notifyRingBuffer[i], observedPipe[i]);
		}
	}

	private void monitorSinglePipe(Pipe<PipeMonitorSchema> output, Pipe<?> localObserved) {

		//if we can't write then do it again on the next cycle, and skip this data point.
		if (Pipe.hasRoomForWrite(output)) {
									
			final int size = Pipe.addMsgIdx(output, MSG_RINGSTATSAMPLE_100);
	
			Pipe.addLongValue(System.currentTimeMillis(), output);
			Pipe.addLongValue(Pipe.headPosition(localObserved), output);
			Pipe.addLongValue(Pipe.tailPosition(localObserved), output);
			Pipe.addIntValue(localObserved.lastMsgIdx, output);
			Pipe.addIntValue(localObserved.sizeOfSlabRing, output);
			Pipe.addLongValue(Pipe.totalWrittenFragments(localObserved), output);

			Pipe.confirmLowLevelWrite(output, size);
			Pipe.publishWrites(output);
						
		} else {
			
			//if unable to write then the values are dropped.
			if (Long.numberOfLeadingZeros(dropped)!=Long.numberOfLeadingZeros(++dropped)) {			
				logger.info("Telemetry is not consuming collected data fast enough dropped:{} rate:{}ns  {}",dropped,(Number)GraphManager.getNota(gm, this, GraphManager.SCHEDULE_RATE, -1),output);
			}
			//if this is happening we probably have a blocking stage which does not release the thread??
			
			
		}
	}

	public Pipe<?> getObservedPipeForOutputId(int id) {
		int i = notifyRingBuffer.length;
		while (--i>=0) {
			if (id == notifyRingBuffer[i].id) {
				return observedPipe[i];
			}
		}
		throw new UnsupportedOperationException();
	}

}
