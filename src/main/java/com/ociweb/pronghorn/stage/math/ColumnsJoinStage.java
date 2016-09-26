package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ColumnsJoinStage<M extends MatrixSchema> extends PronghornStage{

	private Pipe<ColumnSchema<M>>[] columnPipeInput;
	private Pipe<M> matrixPipeOutput;
	private M matrixSchema;
	private int shutdownCount;
	private final int typeSize;
	private final int columnLimit;
	private final int rowLimit;
	
	protected ColumnsJoinStage(GraphManager graphManager, M matrixSchema, Pipe<ColumnSchema<M>>[] columnPipeInput, Pipe<M> matrixPipeOutput) {
		super(graphManager, columnPipeInput, matrixPipeOutput);
		this.columnPipeInput = columnPipeInput;
		this.matrixPipeOutput = matrixPipeOutput;	
		this.matrixSchema = matrixSchema;
		this.shutdownCount = columnPipeInput.length;
		this.typeSize = matrixSchema.typeSize;
		this.rowLimit = matrixSchema.getRows();
		this.columnLimit = matrixSchema.getColumns();
	}
	
	@Override
	public void run() {
		while (Pipe.hasRoomForWrite(matrixPipeOutput) && allHaveContentToRead(columnPipeInput)) {
			
			int c = columnPipeInput.length;
			while (--c>=0) {
				int matrixId = Pipe.takeMsgIdx(columnPipeInput[c]);
				if (matrixId<0) {
					Pipe.confirmLowLevelRead(columnPipeInput[c], Pipe.EOF_SIZE);
					Pipe.releaseReadLock(columnPipeInput[c]);
					shutdownCount--;
				} 
			}
			if (shutdownCount<=0) {
				//we already know there is room for output on this pipe from the above check.
				Pipe.publishEOF(matrixPipeOutput);
				
				requestShutdown();
				return;				
			}
			assert(shutdownCount == columnPipeInput.length);
			
			int matrixSize = Pipe.addMsgIdx(matrixPipeOutput, matrixSchema.matrixId);
			
			
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
					
			
			long targetLoc = Pipe.workingHeadPosition(matrixPipeOutput); 
			for(int rowIdx=0;rowIdx<rowLimit;rowIdx++) {
				for(int columnIdx=0;columnIdx<columnLimit;columnIdx++) {
						
					long sourceLoc = Pipe.getWorkingTailPosition(columnPipeInput[columnIdx]);
					
					int value = Pipe.readValue(0,columnPipeInput[columnIdx] );
					
					Pipe.copyIntsFromToRing(Pipe.slab(columnPipeInput[columnIdx]), (int)sourceLoc, Pipe.slabMask(columnPipeInput[columnIdx]), 
							                Pipe.slab(matrixPipeOutput), (int)targetLoc, Pipe.slabMask(matrixPipeOutput), typeSize);
					
					Pipe.setWorkingTailPosition(columnPipeInput[columnIdx], sourceLoc+(long)typeSize);					
					targetLoc+=(long)typeSize;					
											
				}
			}
			Pipe.setWorkingHead(matrixPipeOutput, targetLoc);
			
			Pipe.confirmLowLevelWrite(matrixPipeOutput, matrixSize);
			Pipe.publishWrites(matrixPipeOutput);
			
			c = columnPipeInput.length;
			while (--c>=0) {
				Pipe.confirmLowLevelRead(columnPipeInput[c], Pipe.sizeOf(columnPipeInput[c], matrixSchema.columnId));
				Pipe.releaseReadLock(columnPipeInput[c]);
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
