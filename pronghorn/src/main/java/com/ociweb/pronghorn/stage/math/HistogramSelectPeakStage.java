package com.ociweb.pronghorn.stage.math;

import java.util.Arrays;
import java.util.Comparator;

import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class HistogramSelectPeakStage extends PronghornStage {

	private final Pipe<HistogramSchema> input;
	private final Pipe<ProbabilitySchema> output;
	
	//lazy initialize based on load
	private long[][] sortWorkspace;
	
	
	private final Comparator<long[]> comp = new Comparator<long[]>() {
		@Override
		public int compare(long[] arg0, long[] arg1) {						
			return Long.compare(arg1[0], arg0[0]);
		}
	};

	public static HistogramSelectPeakStage newInstance(GraphManager gm, Pipe<HistogramSchema> input, Pipe<ProbabilitySchema> output) {
		return new HistogramSelectPeakStage(gm,input,output);
	}
	
	protected HistogramSelectPeakStage(GraphManager graphManager,
									   Pipe<HistogramSchema> input, 
									   Pipe<ProbabilitySchema> output) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
	}

	
	@Override
	public void run() {
		
		while (Pipe.hasContentToRead(input) && Pipe.hasRoomForWrite(output)) {

			int msgIdx = Pipe.takeMsgIdx(input);
			
			if (msgIdx>=0) {
								
				int totalBuckets = Pipe.takeInt(input);	
				
				if (null==sortWorkspace || sortWorkspace.length<totalBuckets) {
					
					sortWorkspace = new long[totalBuckets][];
					int c = totalBuckets;
					while (--c>=0) {
						sortWorkspace[c] = new long[2];					
					}
					
				}
							
				ChannelReader reader = Pipe.openInputStream(input);
				
				long totalSum = 0;
				for(int c=0; c<totalBuckets; c++) {				
					long value = reader.readPackedLong();						
					sortWorkspace[c][0] = value;
					sortWorkspace[c][1] = 0;
					totalSum += value;
				}
				
				//NOTE: this orders the buckets from largest to smallest.
				Arrays.sort(sortWorkspace, comp); 
				
				sendOrderedResults(totalBuckets, totalSum);		
				
				Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIdx));
					
			} else {				
				Pipe.publishEOF(output);								
				Pipe.confirmLowLevelRead(input, Pipe.EOF_SIZE);
			}
			
			Pipe.releaseReadLock(input);
		}
	}

	private void sendOrderedResults(int totalBuckets, long totalSum) {
		int size = Pipe.addMsgIdx(output, ProbabilitySchema.MSG_SELECTION_1);
		
		Pipe.addLongValue(totalSum, output);
		Pipe.addIntValue(totalBuckets, output);
		
		DataOutputBlobWriter<ProbabilitySchema> writer = Pipe.openOutputStream(output);
		for(int c=0;c<totalBuckets;c++) {
			writer.writePackedLong(sortWorkspace[c][0]); //count for this
			writer.writePackedLong(sortWorkspace[c][1]); //index location for this
		}
		DataOutputBlobWriter.closeLowLevelField(writer);

		Pipe.confirmLowLevelWrite(output, size);
		Pipe.publishWrites(output);
	}



}
