package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Merge one histogram from each input pipe together into a single histogram.
 * @author Nathan Tippy
 *
 */
public class HistogramSumStage extends PronghornStage {

	private final Pipe<HistogramSchema>[] inputs;
	private final Pipe<HistogramSchema> output;       
	

	public static HistogramSumStage newInstance(GraphManager gm, 
			                                    Pipe<HistogramSchema> output, 
			                                    Pipe<HistogramSchema> ... inputs) {
		return new HistogramSumStage(gm, output, inputs);
	}
	
	protected HistogramSumStage(GraphManager graphManager,
			                    Pipe<HistogramSchema> output,
			                    Pipe<HistogramSchema> ... inputs) {
		
		super(graphManager, inputs, output);
		this.inputs = inputs;
		this.output = output;
		
	}

	@Override
	public void run() {

		//System.err.println("xxxxxxxxxxxx "+Pipe.hasRoomForWrite(output)+" "+allHaveData(inputs) );
		
		while (Pipe.hasRoomForWrite(output) && allHaveData(inputs)) {
	
			boolean shutDown = false;
			int bucketCounts = 0;
			int msgIdx = -1;
			//open all and read counts
			int i = inputs.length;
			while (--i>=0) {
				
				Pipe<HistogramSchema> pipe = inputs[i];
				msgIdx = Pipe.takeMsgIdx(pipe);
				if (msgIdx>=0) {
					int count = Pipe.takeInt(pipe);
					assert(0==bucketCounts || count==bucketCounts) : "all inputs must agree on buckets count";
					bucketCounts = Math.max(bucketCounts, count);
					Pipe.openInputStream(pipe);
				} else {
					shutDown = true;
				}
			}
					
			if (shutDown) {
				Pipe.publishEOF(output);
			} else {
				Pipe.addMsgIdx(output, HistogramSchema.MSG_HISTOGRAM_1);
				Pipe.addIntValue(bucketCounts, output);
				DataOutputBlobWriter<HistogramSchema> outputStream = Pipe.openOutputStream(output);
				
				int c = bucketCounts;
				while (--c>=0) {
					long total = 0;
					int j = inputs.length;
					while (--j>=0) {
						total += Pipe.inputStream(inputs[j]).readPackedLong();
					}				
					outputStream.writePackedLong(total);
				}
				DataOutputBlobWriter.closeLowLevelField(outputStream);
				Pipe.confirmLowLevelWrite(output, Pipe.sizeOf(HistogramSchema.instance, msgIdx));
				Pipe.publishWrites(output);	
			}
			
			
			int j = inputs.length;
			while (--j>=0) {				
				Pipe.confirmLowLevelRead(inputs[j], Pipe.sizeOf(HistogramSchema.instance, msgIdx));
				Pipe.releaseReadLock(inputs[j]);
			}			
		}
	}

	private boolean allHaveData(Pipe<HistogramSchema>[] inputs) {
		int i = inputs.length;
		while (--i >= 0) {
				
			//System.err.println("i "+i);
				
			if (!Pipe.hasContentToRead(inputs[i])) {
				return false;
			}
		}
		return true;
	}


}
