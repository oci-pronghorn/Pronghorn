package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.math.BuildMatrixCompute.MatrixTypes;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 * Converts data in matrix to decimal data onto the matrix.
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ConvertToDecimalStage<M extends MatrixSchema<M>> extends PronghornStage {

	private final Pipe<RowSchema<M>> input;
	private final Pipe<DecimalSchema<M>> output;
	private final MatrixTypes inputType;
	private final int blockSize;

	/**
	 *
	 * @param graphManager
	 * @param input _in_ Input pipe as matrix with RowSchema
	 * @param output _out_ Output pipe in which RowSchema is converted to DecimalSchema
	 */
	public ConvertToDecimalStage(GraphManager graphManager, Pipe<RowSchema<M>> input, Pipe<DecimalSchema<M>> output) {
		super(graphManager, input, output);
		
		this.input = input;
		this.output = output;
		M schema = (M) input.config().schema();
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
