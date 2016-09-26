package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ColumnsRouteStage<M extends MatrixSchema> extends PronghornStage {

	private final Pipe<M> matrixPipeInput; 
	private final Pipe<ColumnSchema<M>>[] columnPipeOutput;
	private final M matrixSchema;
	
	protected ColumnsRouteStage(GraphManager graphManager, M matrixSchema, Pipe<M> matrixPipeInput, Pipe<ColumnSchema<M>>[] columnPipeOutput) {
		super(graphManager, matrixPipeInput, columnPipeOutput);
		this.matrixPipeInput = matrixPipeInput;
		this.columnPipeOutput = columnPipeOutput;
		this.matrixSchema = matrixSchema;
	}

	@Override
	public void run() {
		
		int columnIdx = 0;
		final int columnLimit = columnPipeOutput.length;
		int rowIdx = 0;
		final int rowLimit = matrixSchema.getRows();
		
		final int typeSize = matrixSchema.typeSize;
		
		while (Pipe.hasContentToRead(matrixPipeInput) && allHaveRoomToWrite(columnPipeOutput)) {
				int matrixId = Pipe.takeMsgIdx(matrixPipeInput);
				if (matrixId<0) {
					
					int c = columnLimit;
					while (--c>=0) {
						Pipe.spinBlockForRoom(columnPipeOutput[c], Pipe.EOF_SIZE);
						Pipe.publishEOF(columnPipeOutput[c]);
					}
					
					
					Pipe.confirmLowLevelRead(matrixPipeInput, Pipe.EOF_SIZE);
					Pipe.releaseReadLock(matrixPipeInput);
					requestShutdown();
					return;
				}
				
				
				int c = columnLimit;
				while (--c>=0) {
					Pipe.addMsgIdx(columnPipeOutput[c], matrixSchema.columnId);
				}				 
				
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
				
				
				long sourceLoc = Pipe.getWorkingTailPosition(matrixPipeInput);
				for(rowIdx=0;rowIdx<rowLimit;rowIdx++) {
					for(columnIdx=0;columnIdx<columnLimit;columnIdx++) {
							
						long targetLoc = Pipe.workingHeadPosition(columnPipeOutput[columnIdx]);
						
						Pipe.copyIntsFromToRing(Pipe.slab(matrixPipeInput), (int)sourceLoc, Pipe.slabMask(matrixPipeInput), 
								                Pipe.slab(columnPipeOutput[columnIdx]), (int)targetLoc, Pipe.slabMask(columnPipeOutput[columnIdx]), typeSize);

						sourceLoc += (long)typeSize;
						Pipe.setWorkingHead(columnPipeOutput[columnIdx], targetLoc+(long)typeSize);
												
					}
				}
				Pipe.setWorkingTailPosition(matrixPipeInput, sourceLoc);
				
				//always confirm writes before reads.
				c = columnLimit;
				while (--c>=0) {
					Pipe.confirmLowLevelWrite(columnPipeOutput[c], Pipe.sizeOf(columnPipeOutput[c], matrixSchema.columnId));					
					Pipe.publishWrites(columnPipeOutput[c]);					
				} 
				
				Pipe.confirmLowLevelRead(matrixPipeInput, Pipe.sizeOf(matrixPipeInput, matrixId));
				Pipe.releaseReadLock(matrixPipeInput);
			
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
