package com.ociweb.pronghorn.stage.math;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.math.BuildMatrixCompute.MatrixTypes;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Commputes matrices based on columns and a single row.
 * @param <M>
 * @param <C>
 * @param <R>
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ColumnComputeStage<M extends MatrixSchema<M>, C extends MatrixSchema<C>, R extends MatrixSchema<R> > extends PronghornStage {

	private Logger logger = LoggerFactory.getLogger(ColumnComputeStage.class);
	
	private Pipe<ColumnSchema<C>>[] colInput;
	private Pipe<RowSchema<R>> rowInput;
	private Pipe<ColumnSchema<M>>[] colOutput;
	private M resultSchema;
	private int rRows;
	private MatrixTypes type;
	private final int colInMsgSize;
	private final int colOutMsgSize;
	
	private boolean shutdownInProgress;
	
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
			                     M matrixSchema, C cSchema, int rRows, int rCols, MatrixTypes type) {
		
		super(graphManager, join(colInput, rowInput), colOutput);
		this.colInput = colInput;
		this.rowInput = rowInput;
		this.colOutput= colOutput;
		this.resultSchema = matrixSchema;
		this.rRows = rRows;
		this.type = type;
				
		assert(colInput.length == colOutput.length);

		this.rowLimit = resultSchema.getRows();		
		this.remainingRows = rowLimit; 
		
		if (cSchema.getColumns() != rRows) {
			throw new UnsupportedOperationException("column count of left input must match row count of right input "+cSchema.getColumns()+" vs "+rRows);
		}
		
		if (resultSchema.getColumns() != rCols) {
			throw new UnsupportedOperationException("column count of right input must match result output "+ rCols+" vs "+resultSchema.getColumns());
		}
			
        if (resultSchema.getRows() != cSchema.getRows()) {
        	throw new UnsupportedOperationException("rows count of left input must match result output "+cSchema.getColumns()+" vs "+resultSchema.getRows());
		}
		        
		if (cSchema.type != matrixSchema.type) {
			throw new UnsupportedOperationException("type mismatch");
		}
		
		if (type != matrixSchema.type) {
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
	public void shutdown() {
		
		Pipe.publishEOF(colOutput);
		
	}
	
	
	@Override
	public void run() {
			
   	 if(shutdownInProgress) {
    	 int i = colOutput.length;
         while (--i >= 0) {
         	if (null!=colOutput[i] && Pipe.isInit(colOutput[i])) {
         		if (!Pipe.hasRoomForWrite(colOutput[i], Pipe.EOF_SIZE)){ 
         			return;
         		}  
         	}
         }
         requestShutdown();
         return;
	 }

	//	System.err.println("____________ ENTER "+remainingRows+"  "+rowLimit+"  "+rowInput+" has content "+Pipe.hasContentToRead(rowInput,Pipe.EOF_SIZE)+" peek "+Pipe.peekInt(rowInput));
			
		
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

		while (Pipe.hasContentToRead(rowInput,Pipe.EOF_SIZE) && 				
				((-1 == Pipe.peekInt(rowInput)) ||
				 ((remainingRows<rowLimit)||(allHaveContentToRead(colInput)&&allHaveRoomToWrite(colOutput)) ) )) {
			
			
			if (Pipe.takeMsgIdx(rowInput) < 0) {				
				Pipe.confirmLowLevelRead(rowInput, Pipe.EOF_SIZE);
				Pipe.releaseReadLock(rowInput);	
				shutdownInProgress = true;
				return;

			}		
			
			if (remainingRows==rowLimit) {		
				int c = colInput.length;
				
				while (--c>=0) {	
					
					if (Pipe.takeMsgIdx(colInput[c])<0) {						
						Pipe.confirmLowLevelRead(colInput[c], Pipe.EOF_SIZE);
						Pipe.releaseReadLock(colInput[c]);							
						shutdownInProgress = true;
						return;		
					} else {
						//only begin the new output column if we did NOT receive the shutdown message.
						Pipe.addMsgIdx(colOutput[c], resultSchema.columnId);
					}
				}

			}
			
			
			remainingRows--;

			
			boolean doNative = false;
			if (doNative) {
				vectorOperations2();
			} else {
				vectorOperations();
			}
			
			Pipe.confirmLowLevelRead(rowInput, Pipe.sizeOf(rowInput, resultSchema.rowId));
			Pipe.releaseReadLock(rowInput);
			
			//System.err.println("z "+remainingRows+"   "+rowInput);
			
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
		
//		System.err.println("____________ EXIT");
				
	}

	private void vectorOperations() {
		long rowSourceLoc = Pipe.getWorkingTailPosition(rowInput);	
		int i = colInput.length;
		while (--i>=0) {
			long sourceLoc = Pipe.getWorkingTailPosition(colInput[i]);	
			Pipe.setWorkingTailPosition(rowInput, rowSourceLoc);				

			//add one value to the output pipe
			//value taken from full rowInput and full inputPipe input				
			type.computeColumn(rRows, colInput[i], rowInput, colOutput[i]);

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
		
		long len = rRows*resultSchema.typeSize;	

		while (--i>=0) {
			
			assert(slabMask == Pipe.slabMask(colInput[i]));
			assert(outMask == Pipe.slabMask(colOutput[i]));
			
			inputPipes[i] = Pipe.slab(colInput[i]);
			outputPipes[i] = Pipe.slab(colOutput[i]);
			cPos[i] = (int)Pipe.getWorkingTailPosition(colInput[i]);
			cPosOut[i] = (int)Pipe.workingHeadPosition(colOutput[i]);
					
			Pipe.setWorkingTailPosition(rowInput, rowSourceLoc+len);
			Pipe.setWorkingHead(colOutput[i], cPosOut[i]+resultSchema.typeSize);
	
			if (remainingRows==0) {  
				Pipe.setWorkingTailPosition(colInput[i], Pipe.getWorkingTailPosition(colInput[i])+len);
			}
			/////
		}		
		
		goCompute(type.ordinal(), Pipe.slab(rowInput), rowSourceLoc, Pipe.slabMask(rowInput), rRows, 
				  inputPipes, cPos, slabMask, outputPipes, cPosOut, outMask);
		
	}
	
	private void goCompute(int typeMask, 
			               int[] rowSlab, long rowPosition, int rowMask, int length, 
			               int[][] colSlabs, int[] colPositions, int colMask, 
			               int[][] outputPipes, int[] cPosOut, int outMask) {
		
		// typeMask == 0 for Integers.
		// typeMask == 1 for Floats.
		// typeMask == 2 for Longs.
	    // typeMask == 3 for Doubles.
		// typeMask == 4 for Decimals.
		
				
		
		//10/5 - profiler shows this block is over 90% of the compute time.
		int p = length;
		while (--p>=0) {
			
			int idx = rowMask&(int)(rowPosition+p);
			int v1 = rowSlab[idx];
			
			int c = colSlabs.length;
			while (--c>=0) {
				int[] is = colSlabs[c];
				int v2 = is[colMask&(colPositions[c]+p)];				
				int prod = v1*v2;				
				outputPipes[c][cPosOut[c]&outMask] += prod;
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
	
	long x=0;
	long y=0;
	
	private boolean allHaveContentToRead(Pipe<ColumnSchema<C>>[] columnPipeInput) {
		int i = columnPipeInput.length;
		
		while (--i>=0) {
			if (!Pipe.hasContentToRead(columnPipeInput[i])) {
				x++;
				return false;
			}
		}
		y++;
		return true;
	}
	
	
	
}
