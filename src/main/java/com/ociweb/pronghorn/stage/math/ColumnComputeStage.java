package com.ociweb.pronghorn.stage.math;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.math.BuildMatrixCompute.MatrixTypes;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ColumnComputeStage<M extends MatrixSchema, C extends MatrixSchema, R extends MatrixSchema > extends PronghornStage {

	private Logger logger = LoggerFactory.getLogger(ColumnComputeStage.class);
	
	private Pipe<ColumnSchema<C>>[] colInput;
	private Pipe<R> matrixInput;
	private Pipe<ColumnSchema<M>>[] colOutput;
	private M resultSchema;
	private R rSchema;
	private MatrixTypes type;
	private int shutdownCount;
	private final int colMsgSize;
	private final int matMsgSize;
	private int activeCol;
		
	
	protected ColumnComputeStage(GraphManager graphManager, Pipe<ColumnSchema<C>>[] colInput,  Pipe<R> matrixInput, Pipe<ColumnSchema<M>>[] colOutput,
			                     M matrixSchema, C cSchema, R rSchema) {
		super(graphManager, join(colInput,matrixInput), colOutput);
		this.colInput = colInput;
		this.matrixInput = matrixInput;
		this.colOutput= colOutput;
		this.resultSchema = matrixSchema;
		this.rSchema = rSchema;
		this.type = rSchema.type;
		this.activeCol = -2;
		
		assert(colInput.length == colOutput.length);
		this.shutdownCount = colInput.length;

		this.colMsgSize = Pipe.sizeOf(colInput[0], matrixSchema.columnId);
		this.matMsgSize = Pipe.sizeOf(matrixInput, matrixSchema.matrixId);
		
		
		if (cSchema.getColumns() != rSchema.getRows()) {
			throw new UnsupportedOperationException("column count of left input must match row count of right input "+cSchema.getColumns()+" vs "+rSchema.getRows());
		}
		
		if (resultSchema.getColumns() != rSchema.getColumns()) {
			throw new UnsupportedOperationException("column count of right input must match result output "+rSchema.getColumns()+" vs "+resultSchema.getColumns());
		}
			
        if (resultSchema.getRows() != cSchema.getRows()) {
        	throw new UnsupportedOperationException("rows count of left input must match result output "+cSchema.getColumns()+" vs "+resultSchema.getRows());
		}
		        
		if (cSchema.type != matrixSchema.type) {
			throw new UnsupportedOperationException("type mismatch");
		}
		
		if (rSchema.type != matrixSchema.type) {
			throw new UnsupportedOperationException("type mismatch");
		}
		        
	}

	long matrixStartPos;
	
	@Override
	public void run() {
		
		//logger.info("{} {} {} {} ",Pipe.hasContentToRead(this.matrixInput),  Pipe.hasContentToRead(this.colInput),  Pipe.hasRoomForWrite(this.colOutput), this.colOutput);
						
		
		if (activeCol == -2) {			
			if (Pipe.hasContentToRead(matrixInput)) {
				int matrixId = Pipe.takeMsgIdx(matrixInput);
				if (matrixId < 0) {
					requestShutdown();
					return;
				}				
				matrixStartPos = ((int)Pipe.getWorkingTailPosition(matrixInput));												
				activeCol = colInput.length-1;//begin next pass			
				
			} else {
				return;//do later when we have content
			}			
		}
					
		Pipe<ColumnSchema<C>> inputPipe;
		Pipe<ColumnSchema<M>> outputPipe;
		
		while (activeCol >= 0 &&
			   Pipe.hasContentToRead(inputPipe = colInput[activeCol]) &&
			   Pipe.hasRoomForWrite(outputPipe = colOutput[activeCol])) {
						
			//start here for the top of each matrix at the top of each column.
			Pipe.setWorkingTailPosition(matrixInput, matrixStartPos);
			
			int colId = Pipe.takeMsgIdx(inputPipe);			
			
			if (colId>=0) {
				int msgOutSize = Pipe.addMsgIdx(outputPipe, resultSchema.columnId);
				
				final long colStartPos = ((int)Pipe.getWorkingTailPosition(inputPipe));
								
				//Due to strong typing we know how long the columns are and how big every matrix is

				//process this once for each row in the exported matrix
				int k = this.resultSchema.getRows();
				while (--k>=0) {	
										
					////////////////
					//apply this column to the full matrix row
					//produce a single value for the output column
					////////////////
					
				    Pipe.setWorkingTailPosition(inputPipe, colStartPos);//reset to the beginning of column.	
				    type.computeColumn(rSchema.getRows(), inputPipe, matrixInput, outputPipe);
   				    
				}				
	            //we have now finished 1 column of the result.
													
				//always confirm writes before reads.
				Pipe.confirmLowLevelWrite(outputPipe, msgOutSize);
				Pipe.publishWrites(outputPipe);
			    
				Pipe.confirmLowLevelRead(inputPipe, colMsgSize); 
				Pipe.releaseReadLock(inputPipe);
				
			} else {
				//we already know that colOuput has room for write
				Pipe.publishEOF(outputPipe);				
				if (--shutdownCount<=0) {
					requestShutdown();
					return;
				}
			}
			//move to next column only after this column is complete
			activeCol--;
		}
			
		//will be -1 when each column has been processed.
		if (activeCol == -1) {
			
			Pipe.confirmLowLevelRead(matrixInput, matMsgSize);
			Pipe.releaseReadLock(matrixInput);		
			activeCol = -2;
		}
		
		
		
	}

}
