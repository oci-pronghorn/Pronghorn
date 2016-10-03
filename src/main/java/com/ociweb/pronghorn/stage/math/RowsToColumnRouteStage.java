package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class RowsToColumnRouteStage<M extends MatrixSchema> extends PronghornStage {

	private final Pipe<RowSchema<M>> rowPipeInput; 
	private final Pipe<ColumnSchema<M>>[] columnPipeOutput;
	private final M matrixSchema;
	private final int rowLimit;
	private int remainingRows;
	
	protected RowsToColumnRouteStage(GraphManager graphManager, M matrixSchema, Pipe<RowSchema<M>> rowPipeInput, Pipe<ColumnSchema<M>>[] columnPipeOutput) {
		super(graphManager, rowPipeInput, columnPipeOutput);
		this.rowPipeInput = rowPipeInput;
		this.columnPipeOutput = columnPipeOutput;
		this.matrixSchema = matrixSchema;
		this.rowLimit = matrixSchema.getRows();
		this.remainingRows = rowLimit;
	}

	@Override
	public void run() {
		
		int columnIdx = 0;
		final int columnLimit = columnPipeOutput.length;
		final int typeSize = matrixSchema.typeSize;

		
		while (Pipe.hasContentToRead(rowPipeInput) && ((remainingRows<rowLimit)||allHaveRoomToWrite(columnPipeOutput))  ) {
						
			    ///////////////////////////
				//begin processing this row
			    ////////////////
				int rowId = Pipe.takeMsgIdx(rowPipeInput);
				if (rowId<0) {
					assert(remainingRows==rowLimit);
					int c = columnLimit;
					while (--c>=0) {
						Pipe.spinBlockForRoom(columnPipeOutput[c], Pipe.EOF_SIZE);
						Pipe.publishEOF(columnPipeOutput[c]);
					}
					
					
					Pipe.confirmLowLevelRead(rowPipeInput, Pipe.EOF_SIZE);
					Pipe.releaseReadLock(rowPipeInput);
					requestShutdown();
					return;
				}
				
				//////////////////////
				//If we are on the first row then open the output column messages
				//////////////////////
				if (remainingRows==rowLimit) {
					int c = columnLimit;
					while (--c>=0) {
						Pipe.addMsgIdx(columnPipeOutput[c], matrixSchema.columnId);
					}				 
				}
				remainingRows--;
				
				//////////////////////////////////
				//splits row oriented matrix input into columns
				/////
				//    [1  2  3
				//     4  5  6]  
				////
				//becomes
				///
				//    [1  4]
				//    [2  5]
				//    [3  6]
						
				long sourceLoc = Pipe.getWorkingTailPosition(rowPipeInput);				
				for(columnIdx=0;columnIdx<columnLimit;columnIdx++) {
						
					long targetLoc = Pipe.workingHeadPosition(columnPipeOutput[columnIdx]);
					
					Pipe.copyIntsFromToRing(Pipe.slab(rowPipeInput), (int)sourceLoc, Pipe.slabMask(rowPipeInput), 
							                Pipe.slab(columnPipeOutput[columnIdx]), (int)targetLoc, Pipe.slabMask(columnPipeOutput[columnIdx]), typeSize);

					sourceLoc += (long)typeSize;
					Pipe.setWorkingHead(columnPipeOutput[columnIdx], targetLoc+(long)typeSize);
											
				}				
				Pipe.setWorkingTailPosition(rowPipeInput, sourceLoc);
				
				
				//always confirm writes before reads.
				if (remainingRows==0) {
								
					int c = columnLimit;
					while (--c>=0) {
						int sizeOf = Pipe.sizeOf(columnPipeOutput[c], matrixSchema.columnId);
						//System.out.println("wrote col "+sizeOf);
						Pipe.confirmLowLevelWrite(columnPipeOutput[c], sizeOf);					
						Pipe.publishWrites(columnPipeOutput[c]);					
					}
					remainingRows = rowLimit;
				}
				
				//release this row we are done processing it against all the columns
				Pipe.confirmLowLevelRead(rowPipeInput, Pipe.sizeOf(rowPipeInput, rowId));
				Pipe.releaseReadLock(rowPipeInput);
			
		}
		
		
	}

	private boolean allHaveRoomToWrite(Pipe<ColumnSchema<M>>[] columnPipeOutput) {
		int i = columnPipeOutput.length;
		while (--i>=0) {
			if (!Pipe.hasRoomForWrite(columnPipeOutput[i])) {
				return false;
			}
		}
		return true;
	}

}
