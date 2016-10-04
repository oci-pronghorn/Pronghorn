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
	private Pipe<RowSchema<R>> rowInput;
	private Pipe<ColumnSchema<M>>[] colOutput;
	private M resultSchema;
	private R rSchema;
	private MatrixTypes type;
	private int shutdownCount;
	private final int colInMsgSize;
	private final int colOutMsgSize;
	
	
	private final int rowLimit;
	private int remainingRows;
	
	private int[][] inputPipes;
	private int[][] outputPipes;
	private int[]   cPos;
	private int[]   cPosOut;

	
	protected ColumnComputeStage(GraphManager graphManager, 
			                     Pipe<ColumnSchema<C>>[] colInput, //input matrix split into columns
			                     Pipe<RowSchema<R>>      rowInput, //input matrix split into rows
			                     Pipe<ColumnSchema<M>>[] colOutput,//output matrix split into columns
			                     M matrixSchema, C cSchema, R rSchema) {
		
		super(graphManager, join(colInput, rowInput), colOutput);
		this.colInput = colInput;
		this.rowInput = rowInput;
		this.colOutput= colOutput;
		this.resultSchema = matrixSchema;
		this.rSchema = rSchema;
		this.type = rSchema.type;
				
		assert(colInput.length == colOutput.length);
		this.shutdownCount = colInput.length+1;//plus one for row pipe

		this.rowLimit = resultSchema.getRows();		
		this.remainingRows = rowLimit; 
		
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
		
		if (colInput.length != colOutput.length) {
			throw new UnsupportedOperationException("expected counts to match "+colInput.length+" != "+colOutput.length);
		}
		
		this.colInMsgSize = Pipe.sizeOf(colInput[0], matrixSchema.columnId);
		this.colOutMsgSize = Pipe.sizeOf(colOutput[0], matrixSchema.columnId);
		
	}
	
	
	@Override
	public void startup() {
		inputPipes = new int[colInput.length][];
		outputPipes = new int[colInput.length][];
		cPos = new int[colInput.length];
		cPosOut = new int[colInput.length];
	}
	
	@Override
	public void run() {
			
		//read columns from pipes - all columns must be present and walked multiple times until done
		//read rows from a single pipe - process each as they come in 
		//write out rows to a single pipe - send out 1 completed row as they are finished
		
		// A1  B1  C1       X1
		// A2  B2  C2  *    Y1              
		//                  Z1   
		//
		//  //two rows out but sent as columns
		//  (A1*X1 + B1*Y1 + C1*Z1)
		//  (A2*X1 + B2*Y1 + C2*Z1)
		//

		while (Pipe.hasContentToRead(rowInput) && 
				((Pipe.peekInt(rowInput)==-1) || //if row has shutdown then do so.
				 ((remainingRows<rowLimit)||(allHaveContentToRead(colInput)&&allHaveRoomToWrite(colOutput)) ) ) ) {
			
			int rowId = Pipe.takeMsgIdx(rowInput);
			
			if (rowId < 0) {
				Pipe.confirmLowLevelRead(rowInput, Pipe.EOF_SIZE);
				Pipe.releaseReadLock(rowInput);
				if (--shutdownCount==0) {
					assert(remainingRows==rowLimit);
					requestShutdown();				
					sendShutdownDownStream();
					logger.trace("shutdown A");
					return;
				}
			}		
			
			if (remainingRows==rowLimit) {		
				int c = colInput.length;
				
				//System.out.println(rowInput.id+" new matrix row input "+rowInput);
				
				
				while (--c>=0) {	
					
					if (Pipe.takeMsgIdx(colInput[c])<0) {
						Pipe.confirmLowLevelRead(colInput[c], Pipe.EOF_SIZE);
						Pipe.releaseReadLock(colInput[c]);
						if (--shutdownCount==0) {
							assert(remainingRows==rowLimit);
							requestShutdown();
							sendShutdownDownStream();	
							logger.trace("shutdown B");
							return;		
						}
					}
					
					Pipe.addMsgIdx(colOutput[c], resultSchema.columnId);
				}

			}
			
			if (shutdownCount<colInput.length+1) {
				logger.trace("shutdown C");
				return;
			}
			remainingRows--;

			
			
			boolean doNative = false;
			if (doNative) {
				vectorOperations2();
			} else {
				vectorOperations();
			}
			
			Pipe.confirmLowLevelRead(rowInput, Pipe.sizeOf(rowInput, rowId));
			Pipe.releaseReadLock(rowInput);
			
			if (0==remainingRows) {

				//System.out.println(rowInput.id+" released matrix row input "+rowInput);
				
				
				//done with the columns so release them
				int  c = colInput.length;
				while (--c>=0) {
					
					Pipe.confirmLowLevelWrite(colOutput[c], colOutMsgSize);
					Pipe.publishWrites(colOutput[c]);
					
					Pipe.confirmLowLevelRead(colInput[c], colInMsgSize);
					Pipe.releaseReadLock(colInput[c]);
	
				}			
				remainingRows = rowLimit;
			}
			
			
		}
		
				
	}

	private void vectorOperations() {
		long rowSourceLoc = Pipe.getWorkingTailPosition(rowInput);	
		int i = colInput.length;
		while (--i>=0) {
			long sourceLoc = Pipe.getWorkingTailPosition(colInput[i]);	
			Pipe.setWorkingTailPosition(rowInput, rowSourceLoc);				

			//add one value to the output pipe
			//value taken from full rowInput and full inputPipe input				
			type.computeColumn(rSchema.getRows(), colInput[i], rowInput, colOutput[i]);

			if (remainingRows>0) {  
				//restore for next pass but not for the very last one.
				Pipe.setWorkingTailPosition(colInput[i],sourceLoc);
			}
			
		}
	}


	
	private void vectorOperations2() {
		long rowSourceLoc = Pipe.getWorkingTailPosition(rowInput);	
		
		int i = colInput.length;
		
		int slabMask = Pipe.slabMask(colInput[0]);
		int outMask = Pipe.slabMask(colOutput[0]);
		
		while (--i>=0) {
			
			assert(slabMask == Pipe.slabMask(colInput[i]));
			assert(outMask == Pipe.slabMask(colOutput[i]));
			
			inputPipes[i] = Pipe.slab(colInput[i]);
			outputPipes[i] = Pipe.slab(colOutput[i]);
			cPos[i] = (int)Pipe.getWorkingTailPosition(colInput[i]);
			cPosOut[i] = (int)Pipe.getWorkingTailPosition(colOutput[i]);
			
			if (remainingRows==0) {//only done on last row
				long sourceLoc = Pipe.getWorkingTailPosition(colInput[i]);
				Pipe.setWorkingTailPosition(colInput[i],sourceLoc + (resultSchema.typeSize*rSchema.getRows()));
			}
		}		
		
		goCompute(type.typeMask, Pipe.slab(rowInput), rowSourceLoc, Pipe.slabMask(rowInput), rSchema.getRows(), inputPipes, cPos, slabMask, outputPipes, cPosOut, outMask);
				
		Pipe.setWorkingTailPosition(rowInput, rowSourceLoc + (resultSchema.typeSize*rSchema.getRows()));
				
	}
	
    //TODO:  YF this is the method to be implemented natively 
	private void goCompute(int typeMask, 
			               int[] rowSlab, long rowPosition, int rowMask, int length, 
			               int[][] colSlabs, int[] colPositions, int colMask, 
			               int[][] outputPipes, int[] cPosOut, int outMask) {
		
		
		// TODO Auto-generated method stub
		
		
		
	}


	private void sendShutdownDownStream() {
		if (remainingRows==rowLimit) {
			int c= colOutput.length;
			while (--c>=0) {
				Pipe.publishEOF(colOutput[c]);
			}
		} else {
			int c= colOutput.length;
			while (--c>=0) {
				Pipe.shutdown(colOutput[c]);
			}
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
	
	private boolean allHaveContentToRead(Pipe<ColumnSchema<C>>[] columnPipeInput) {
		int i = columnPipeInput.length;
		
		while (--i>=0) {
			if (!Pipe.hasContentToRead(columnPipeInput[i])) {
				return false;
			}
		}
		return true;
	}
	
	
	
}
