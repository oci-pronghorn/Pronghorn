package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Stage that converts columns to rows in a matrix.
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ColumnsToRowsStage<M extends MatrixSchema<M>> extends PronghornStage{

	private Pipe<ColumnSchema<M>>[] columnPipeInput;
	private Pipe<RowSchema<M>> matrixPipeOutput;
	private M matrixSchema;
	private int shutdownCount;
	private final int typeSize;
	private final int columnLimit;
	private final int rowLimit;
	private int remainingRows;
	private final int colSizeOf;
	private final int matrixSize;
	
	
    //NOTE: For Neural compute? Improve .. the column converters need a flag to hold and re-use the last value so we only need to xmit once....

	/**
	 *
	 * @param graphManager
	 * @param columnPipeInput _in_ Pipes containing ColumnSchema that will be converted to RowSchema
	 * @param matrixPipeOutput _out_ Pipes onto which ColumnSchema will be output to RowSchema
	 */
	public ColumnsToRowsStage(GraphManager graphManager, Pipe<ColumnSchema<M>>[] columnPipeInput, Pipe<RowSchema<M>> matrixPipeOutput) {
		super(graphManager, columnPipeInput, matrixPipeOutput);
		
		M matrixSchema = (M) matrixPipeOutput.config().schema().rootSchema();
		
		this.columnPipeInput = columnPipeInput;
		this.matrixPipeOutput = matrixPipeOutput;	
		this.matrixSchema = matrixSchema;
		this.shutdownCount = columnPipeInput.length;
		this.typeSize = matrixSchema.typeSize;
		this.rowLimit = matrixSchema.getRows();
		this.columnLimit = matrixSchema.getColumns();
		this.remainingRows = rowLimit;
		assert(columnLimit==columnPipeInput.length);
		this.colSizeOf = Pipe.sizeOf(columnPipeInput[0], matrixSchema.columnId);
		this.matrixSize = Pipe.sizeOf(matrixPipeOutput, matrixSchema.rowId);
		
	}

	public void run() {
		
		while (Pipe.hasRoomForWrite(matrixPipeOutput) && ((remainingRows<rowLimit)||allHaveContentToRead(columnPipeInput))) {
						
			///////////////////////
			//open all the columns for reading since we are on the first row.
			//////////////////////
			if (remainingRows==rowLimit) {		
				assert(allHaveContentToRead(columnPipeInput));
				
				int c = columnPipeInput.length;
				while (--c>=0) {
					if (Pipe.takeMsgIdx(columnPipeInput[c])<0) {
						Pipe.confirmLowLevelRead(columnPipeInput[c], Pipe.EOF_SIZE);
						Pipe.releaseReadLock(columnPipeInput[c]);
						
						shutdownCount--;
					}
				}
				//if any column goes into shutdown then we shutdown them all.
				if (shutdownCount!=columnPipeInput.length) {
					Pipe.publishEOF(matrixPipeOutput);				
					requestShutdown();
					return;				
				}
			}
			
			/////
			//combines columns back into matrix
			////
			//given
			// [1 2]
			// [3 4]
			// [5 6]
			//produces
			// [1 3 5
			//  2 4 6]
			////////
			remainingRows--;
						
			//Write one row			
			Pipe.addMsgIdx(matrixPipeOutput, matrixSchema.rowId);	
			
			long targetLoc = Pipe.workingHeadPosition(matrixPipeOutput); 
		    int tSize = typeSize;
			for(int columnIdx=0;columnIdx<columnLimit;columnIdx++) {
					
				Pipe<ColumnSchema<M>> colPipeIn = columnPipeInput[columnIdx];
				
				long sourceLoc = Pipe.getWorkingTailPosition(colPipeIn);			
		
				Pipe.copyIntsFromToRing(Pipe.slab(colPipeIn), (int)sourceLoc, Pipe.slabMask(colPipeIn), 
						                Pipe.slab(matrixPipeOutput), (int)targetLoc, Pipe.slabMask(matrixPipeOutput), tSize);
				
				Pipe.setWorkingTailPosition(colPipeIn, sourceLoc+(long)tSize);					
				targetLoc+=(long)tSize;					
										
			}						
			Pipe.setWorkingHead(matrixPipeOutput, targetLoc);
			
			Pipe.confirmLowLevelWrite(matrixPipeOutput, matrixSize);
			Pipe.publishWrites(matrixPipeOutput);

			if (0==remainingRows) {
				//done with the columns so release them
				int  c = columnPipeInput.length;
				while (--c>=0) {
					Pipe<ColumnSchema<M>> colPipeIn = columnPipeInput[c];
					Pipe.confirmLowLevelRead(colPipeIn, colSizeOf);
					Pipe.releaseReadLock(colPipeIn);
				}				
				remainingRows = rowLimit;
			}
			
		}
				
	}

	
	private boolean allHaveContentToRead(Pipe<ColumnSchema<M>>[] columnPipeInput) {
		int i = columnPipeInput.length;
		while (--i>=0) {
			if (!Pipe.hasContentToRead(columnPipeInput[i])) {				
				return false;
			}
		}
		return true;
	}
	
}
