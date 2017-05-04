package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.math.BuildMatrixCompute.MatrixTypes;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ConvertToDecimalStage<M extends MatrixSchema<M>> extends PronghornStage {

	private final Pipe<RowSchema<M>> input;
	private final Pipe<DecimalSchema<M>> output;
	private final MatrixTypes inputType;
	private final int blockSize;
	
	public ConvertToDecimalStage(GraphManager graphManager, M schema, Pipe<RowSchema<M>> input, Pipe<DecimalSchema<M>> output) {
		super(graphManager, input, output);
		
		this.input = input;
		this.output = output;
		this.blockSize = schema.getRows()*schema.getColumns();
		this.inputType = schema.type;
	}

	@Override
	public void run() {
		
		while (Pipe.hasContentToRead(input) && Pipe.hasRoomForWrite(output)) {
		
			int msgIn = Pipe.takeMsgIdx(input);
			if (msgIn<0) {
				Pipe.publishEOF(output);
				requestShutdown();
				return;
			}
			
			int msgSize = Pipe.addMsgIdx(output, msgIn);//WARNING: using the same id as we just took in,
			
			inputType.convertToDecimal(blockSize, input, output);
			
			Pipe.confirmLowLevelWrite(output, msgSize);
			Pipe.publishWrites(output);
			
			Pipe.confirmLowLevelRead(input, Pipe.sizeOf(input, msgIn));
			Pipe.releaseReadLock(input);
			
		}
		
	}

}
