package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.route.ReplicatorStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class BuildMatrixCompute {

	/**
	 * TOOD: recursive call for loop (why needed) stop with node TTL depth
	 * TODO: send multiple column pipes in to compute to share copy of matrix.
	 * TODO: shortcut of columns directly.
	 * TODO: Unit test
	 * TODO: test app for server.
	 * TODO: determine different matrix shapes that can be run by the same graph.
	 * 
	 * 
	 * @author Nathan Tippy
	 *
	 */
	
	
	public enum MatrixTypes {
		Integers(TypeMask.IntegerSigned) {
			public void convertToDecimal(int count, Pipe<?> input, Pipe<?> output) {
				while (--count>=0) {
					
					int dotPosition = 0;
					int value = Pipe.takeInt(input);
					while (0 ==	(value%10)) {
						value = value/10;
						dotPosition++;
					}
										
					Pipe.addIntValue(dotPosition, output); //exp
					Pipe.addLongValue(value, output);
				}
			}
			public void computeColumn(int rows, Pipe<?> columnPipe, Pipe<?> rowPipe, Pipe<?> outputPipe) {
	
				int sum = 0; 
				while (--rows >= 0) {
					int colValue = Pipe.takeInt(columnPipe); //take each value from this column
					int matrixValue = Pipe.takeInt(rowPipe); //take each value from the same sized row
					sum = sum + (colValue*matrixValue);
				}
				//sum now holds the new value to be sent out
				Pipe.addIntValue(sum, outputPipe); 
			}

			@Override
			public void addValue(double i, Pipe<?> right) {
				Pipe.addIntValue((int)i, right);
				
			}

			@Override
			public int size() {
				return 1;
			}
		},
		Floats(TypeMask.IntegerSigned) {
			public void convertToDecimal(int count, Pipe<?> input, Pipe<?> output) {
				while (--count>=0) {
					double value = (double)Float.intBitsToFloat(Pipe.takeInt(input));
				
					int dotPosition=0;
					while (0f != value%1f) {
						value = value*10;
						dotPosition--;//move point to left to put it back						
					}
								
					if (value != 0) {
						while (0 ==	(value%10f)) {
							value=value/10f;
							dotPosition++;
						}
					}

					Pipe.addIntValue(dotPosition, output); //safe because long has 18 digits
					Pipe.addLongValue((long)value, output);
			
				}
			}
			public void computeColumn(int rows, Pipe<?> columnPipe, Pipe<?> rowPipe, Pipe<?> outputPipe) {
				float sum = 0; 
				while (--rows >= 0) {
					float colValue = Float.intBitsToFloat(Pipe.takeInt(columnPipe)); //take each value from this column
					float matrixValue = Float.intBitsToFloat(Pipe.takeInt(rowPipe)); //take each value from the same sized row
					sum = sum + (colValue*matrixValue);
				}
				//sum now holds the new value to be sent out
				Pipe.addIntValue(Float.floatToIntBits(sum), outputPipe); 
			}

			@Override
			public void addValue(double i, Pipe<?> right) {
				Pipe.addIntValue(Float.floatToIntBits((float)i), right);
			}

			@Override
			public int size() {
				return 1;
			}
		},
		Longs(TypeMask.LongSigned) {
			public void convertToDecimal(int count, Pipe<?> input, Pipe<?> output) {
				while (--count>=0) {					

					int dotPosition = 0;
					long value = Pipe.takeLong(input);
					while (0 ==	(value%10)) {
						value = value/10;
						dotPosition++;
					}
					
					Pipe.addIntValue(dotPosition, output); //exp
					Pipe.addLongValue(value, output);
				}
			}
			public void computeColumn(int rows, Pipe<?> columnPipe, Pipe<?> rowPipe, Pipe<?> outputPipe) {
				long sum = 0; 
				while (--rows >= 0) {
					long colValue = Pipe.takeLong(columnPipe); //take each value from this column
					long matrixValue = Pipe.takeLong(rowPipe); //take each value from the same sized row
					sum = sum + (colValue*matrixValue);
				}
				//sum now holds the new value to be sent out
				Pipe.addLongValue(sum, outputPipe); 
			}

			@Override
			public void addValue(double i, Pipe<?> right) {
				Pipe.addLongValue((long)i, right);
			}

			@Override
			public int size() {
				return 2;
			}
		},
		Doubles(TypeMask.LongSigned){
			public void convertToDecimal(int count, Pipe<?> input, Pipe<?> output) {
				while (--count>=0) {
					
					double value = (double)Double.longBitsToDouble(Pipe.takeLong(input));
					
					int dotPosition=0;
					while (0d != value%1d) {
						value = value*10;
						dotPosition--;//move point to left to put it back						
					}
					if (value!=0) {					
						while (0 ==	(value%10d)) {
							value = value/10d;
							dotPosition++;
						}
					}

					Pipe.addIntValue(dotPosition, output); //safe because long has 18 digits
					Pipe.addLongValue((long)value, output);
					
				}
			}
			public void computeColumn(int rows, Pipe<?> columnPipe, Pipe<?> rowPipe, Pipe<?> outputPipe) {
				double sum = 0; 
				while (--rows >= 0) {
					double colValue = Double.longBitsToDouble(Pipe.takeLong(columnPipe)); //take each value from this column
					double matrixValue = Double.longBitsToDouble(Pipe.takeLong(rowPipe)); //take each value from the same sized row
					sum = sum + (colValue*matrixValue);
				}
				//sum now holds the new value to be sent out
				Pipe.addLongValue(Double.doubleToLongBits(sum), outputPipe); 
			}

			@Override
			public void addValue(double i, Pipe<?> right) {
				Pipe.addLongValue(Double.doubleToLongBits((double)i), right);
			}

			@Override
			public int size() {
				return 2;
			}
		},
		Decimals(TypeMask.Decimal){
			public void convertToDecimal(int count, Pipe<?> input, Pipe<?> output) {
				long targetLoc = Pipe.workingHeadPosition(output);
				long sourceLoc = Pipe.getWorkingTailPosition(input);
				int len = 3*count;
				Pipe.copyIntsFromToRing(Pipe.slab(input), (int)sourceLoc, Pipe.slabMask(input), 
						                Pipe.slab(output), (int)targetLoc, Pipe.slabMask(output), len);
				Pipe.setWorkingTailPosition(input, sourceLoc+(long)len);					
				Pipe.setWorkingHead(output, targetLoc+(long)len);
			}
			
			
			public void computeColumn(int rows, Pipe<?> columnPipe, Pipe<?> rowPipe, Pipe<?> outputPipe) {
				
				int  sumExp   = 0;
				long sumValue = 0;
				
				while (--rows >= 0) {
					
					int  xExp   = Pipe.takeInt(columnPipe);
					long x      = Pipe.takeLong(columnPipe); //take each value from this column
					
					int yExp   = Pipe.takeInt(rowPipe);
					long y     = Pipe.takeLong(rowPipe); //take each value from the same sized row
					
					/////////////////////
					///multiply
					////////////////////
					
				    long r;
				    boolean overflows = false;
				    
				    do {
				    	
					    r = x * y;	
					    
					    overflows = didOverflow(x, y, r);
					    
					    //if it did overflow reduce the accuracy of the larger value
					    if (overflows) {
					    	if (x>y) {
					    		x = x/10;
					    		xExp++;					    		
					    	} else {
					    		y = y/10;
					    		yExp++;					    		
					    	}					    	
					    }
					 
					    //keep going until we adjust the numbers enough to fit without overflow
				    } while (overflows);
				    
					long prod = r;
					int exp = xExp+yExp;

					///////////////////
					///Add
					///////////////////
					
					//choose the smallest exponent
					if (exp==sumExp) {
						sumValue += prod;
						
					} else if (exp>sumExp){
						int dif = (exp-sumExp);		
						//if dif is > 18 then we will loose the data anyway..  
						long temp =	dif>=longPow.length? 0 : prod*longPow[dif];												
						sumValue = sumValue + temp;
					} else {
						int dif = (sumExp-exp);						
						long temp =	dif>=longPow.length? 0 : sumValue*longPow[dif];
						sumValue = prod+temp;
						sumExp = exp;
					}	
					
				}
				
				///////////////////
				//Recenter
				///////////////////
				
				if (sumValue != 0) {
					while (0 == (sumValue % 10L)) {
						sumValue = sumValue / 10L;
						sumExp++;
					}
				}
				
				//sum now holds the new value to be sent out
				Pipe.addIntValue(sumExp, outputPipe);
				Pipe.addLongValue(sumValue, outputPipe); 
			}


			private boolean didOverflow(long x, long y, long r) {
				//same logic as Math.multiplyExact however we are not throwing instead we use a boolean.
				//for more explantation of why this works check hackers delight
				boolean overflows = false;
				long ax = Math.abs(x);
				long ay = Math.abs(y);
				if (((ax | ay) >>> 31 != 0)) {
				    // Some bits greater than 2^31 that might cause overflow
				    // Check the result using the divide operator
				    // and check for the special case of Long.MIN_VALUE * -1
					overflows = (((y != 0) && (r / y != x)) ||
				               (x == Long.MIN_VALUE && y == -1));
				}
				return overflows;
			}


			  
			@Override
			public void addValue(double i, Pipe<?> right) {
				 
				//keeps 6 places but only if they have value. (TODO: add method so caller can choose accuracy)
				int places = -6;
				long value = (long)Math.rint(i*powd[64-places]);
//				while (places<0 && value%10 == 0) {
//					value = value/10;
//					places++;
//				}
	
				Pipe.addIntValue(places, right);
				Pipe.addLongValue(value, right); 
				
			}


			@Override
			public int size() {
				return 3;
			}
		};
		
		public int typeMask;
		
		private MatrixTypes(int typeMask) {
			this.typeMask=typeMask;
		}

		public abstract void computeColumn(int rows, Pipe<?> columnPipe, Pipe<?> rowPipe, Pipe<?> outputPipe);

		public abstract void addValue(double i, Pipe<?> right);
		
		public abstract void convertToDecimal(int count, Pipe<?> input, Pipe<?> output);		

		public abstract int size();
		
		private static long[] longPow = new long[] {1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 
				                                   1_000_000_000, 10_000_000_000L, 100_000_000_000L ,1000_000_000_000L,
				                                   1_000_000_000_000L, 10_000_000_000_000L, 100_000_000_000_000L ,1000_000_000_000_000L,
				                                   1_000_000_000_000_000L, 10_000_000_000_000_000L, 100_000_000_000_000_000L ,1000_000_000_000_000_000L};
		
		private static double[] powd = new double[] {
				  1.0E-64,1.0E-63,1.0E-62,1.0E-61,1.0E-60,1.0E-59,1.0E-58,1.0E-57,1.0E-56,1.0E-55,1.0E-54,1.0E-53,1.0E-52,1.0E-51,1.0E-50,1.0E-49,1.0E-48,1.0E-47,1.0E-46,
				  1.0E-45,1.0E-44,1.0E-43,1.0E-42,1.0E-41,1.0E-40,1.0E-39,1.0E-38,1.0E-37,1.0E-36,1.0E-35,1.0E-34,1.0E-33,1.0E-32,1.0E-31,1.0E-30,1.0E-29,1.0E-28,1.0E-27,1.0E-26,1.0E-25,1.0E-24,1.0E-23,1.0E-22,
				  1.0E-21,1.0E-20,1.0E-19,1.0E-18,1.0E-17,1.0E-16,1.0E-15,1.0E-14,1.0E-13,1.0E-12,1.0E-11,1.0E-10,1.0E-9,1.0E-8,1.0E-7,1.0E-6,1.0E-5,1.0E-4,0.001,0.01,0.1,1.0,10.0,100.0,1000.0,10000.0,100000.0,1000000.0,
				  1.0E7,1.0E8,1.0E9,1.0E10,1.0E11,1.0E12,1.0E13,1.0E14,1.0E15,1.0E16,1.0E17,1.0E18,1.0E19,1.0E20,1.0E21,1.0E22,1.0E23,1.0E24,1.0E25,1.0E26,1.0E27,1.0E28,1.0E29,1.0E30,1.0E31,1.0E32,1.0E33,1.0E34,1.0E35,
				  1.0E36,1.0E37,1.0E38,1.0E39,1.0E40,1.0E41,1.0E42,1.0E43,1.0E44,1.0E45,1.0E46,1.0E47,1.0E48,1.0E49,1.0E50,1.0E51,1.0E52,1.0E53,1.0E54,1.0E55,1.0E56,1.0E57,1.0E58,1.0E59,1.0E60,1.0E61,1.0E62,1.0E63,1.0E64};

	}
	
	
	public static <M extends MatrixSchema<M>> MatrixSchema<M> buildSchema(int rows, int columns, MatrixTypes type) {
		return new MatrixSchema<M>(rows, columns, type);
	}


	public static MatrixSchema buildResultSchema(MatrixSchema leftSchema, MatrixSchema rightSchema) {
		assert(leftSchema.type == rightSchema.type);
		return buildSchema(leftSchema.getRows(), rightSchema.getColumns(), leftSchema.type);
	}

	private static <M extends MatrixSchema<M>> MatrixSchema resultSchema(Pipe<RowSchema<M>> left,
			Pipe<RowSchema<M>> right) {
		MatrixSchema<M> leftSchema1 = left.config().schema().rootSchema();
		MatrixSchema<M> rightSchema1 = right.config().schema().rootSchema();
		MatrixSchema resultSchema = BuildMatrixCompute.buildResultSchema(leftSchema1, rightSchema1);
		return resultSchema;
	}

	
	public static <M extends MatrixSchema<M>> Pipe<ColumnSchema<M>>[] buildProductGraphRC(GraphManager gm, 
	            		Pipe<RowSchema<M>> leftInput, 
	            		Pipe<RowSchema<M>> rightInput, 
	            		int parallelism) {

		MatrixSchema<M> resultSchema = resultSchema(leftInput, rightInput);

		
		MatrixSchema<M> rightSchema = rightInput.config().schema().rootSchema();
						
		int i = rightSchema.getColumns();

		Pipe<ColumnSchema<M>>[] intputAsColumns = new Pipe[i];
		Pipe<ColumnSchema<M>>[] resultColumnPipes = new Pipe[i];

		ColumnSchema<M> columnsInputSchema = new ColumnSchema<M>(rightSchema);		
		assert(rightSchema.rows == columnsInputSchema.rows);

		ColumnSchema<M> columnsOutoutSchema = new ColumnSchema<M>(resultSchema);

		PipeConfig<ColumnSchema<M>> rightColumnConfig = new PipeConfig<ColumnSchema<M>>(columnsInputSchema,4); //just big enough to support batching of one consumer and one producer
		rightColumnConfig.hideLabels();
		
		
		//TODO: build second compute that will use rows rather than keep this column open.
		PipeConfig<ColumnSchema<M>> resultColumnConfig = new PipeConfig<ColumnSchema<M>>(columnsOutoutSchema,4);
		resultColumnConfig.hideLabels();
		
		int parts = Math.min(parallelism, i);
		int partsSize = i/parts;
		
		//TODO: divide group and spread the remainder evenly so we have groups of 2 sizes and most.
		//      * this is required before we can make the compute stage write to rows.
		//      * the rowTocol and colToRow will be the next pinch point  (colToRows is removed by the above, rowToCall will only be used externally for data generation)
		//      * rowToCol can be removed by producing data in cols in the first place.
		
						
		int splitterPipesCount = parts;
		Pipe<RowSchema<M>>[] splitterPipes = new Pipe[splitterPipesCount];
		
		int start = i;
		while (--i>=0) {
			intputAsColumns[i] = new Pipe<ColumnSchema<M>>(rightColumnConfig);			
			resultColumnPipes[i] =  new Pipe<ColumnSchema<M>>(resultColumnConfig);	
		
			//build each parallel compute stage that will deal with multiple columns, 
			//note how the last one takes the remainder of the pipes. TODO: may want to revist for better spread of the remainder.
			int len = start-i;			
			if ((splitterPipesCount>1 && len==partsSize) || i==0) {
		
				splitterPipesCount = buildComputeStage(gm, resultSchema, rightSchema, leftInput, i, intputAsColumns,
						                               resultColumnPipes, 
						                               splitterPipesCount, 
						                               splitterPipes, start, len);
				start = i;
			}
					
		}

		//split the left matrix into N column pipes.
		new RowsToColumnRouteStage(gm, rightInput, intputAsColumns);
		
		if (splitterPipesCount>1) {
			new ReplicatorStage<RowSchema<M>>(gm, leftInput, splitterPipes); //duplicate the matrix once for each column.		
		}
		
		return resultColumnPipes;
	}


	private static <M extends MatrixSchema<M>> int buildComputeStage(
			GraphManager gm, 
			
			MatrixSchema<M> resultSchema, 
			MatrixSchema<M> rightSchema, 
			
			Pipe<RowSchema<M>> leftInput, 
		
			int i, 
			Pipe<ColumnSchema<M>>[] inputAsColumns,
			Pipe<ColumnSchema<M>>[] resultInColumns, 
			int splitterPipesCount, 
			Pipe<RowSchema<M>>[] splitterPipes,
			int start, 
			int len) {
		
		MatrixSchema<M> leftSchema = leftInput.config().schema().rootSchema(); 
		
		int idx = start;
		Pipe<ColumnSchema<M>>[] inputs = new Pipe[len];
		Pipe<ColumnSchema<M>>[] outputs = new Pipe[len];
		int c = 0;
		while (--idx >= i) {
			inputs[c]=inputAsColumns[idx];
			outputs[c]=resultInColumns[idx];
			c++;
		}
		assert(c==len);

		//if the work is not split then we do not need a replicator.
		Pipe<RowSchema<M>> pipe = 1==splitterPipesCount ?
								  leftInput:
				                  new Pipe<RowSchema<M>>(leftInput.config().grow2x());
		
		new ColumnComputeStage( gm, 
				                inputs, 
				                splitterPipes[--splitterPipesCount] = pipe,
				                outputs, 
				                resultSchema, leftSchema, rightSchema.getRows(), rightSchema.getColumns(), rightSchema.type);
		return splitterPipesCount;
	}


	public static <M extends MatrixSchema<M>> Pipe<RowSchema<M>> buildProductGraphRR(GraphManager gm,
			int targetThreadCount, Pipe<RowSchema<M>> left, Pipe<RowSchema<M>> right) {
		Pipe<ColumnSchema<M>>[] colResults = buildProductGraphRC(gm,
																			left, right,
																			targetThreadCount);
				
		//////////////////
		//////////////////
		
		MatrixSchema resultSchema2 = colResults[0].config().schema().rootSchema();
		PipeConfig<RowSchema<M>> config = new PipeConfig<RowSchema<M>>(new RowSchema<M>(resultSchema2), resultSchema2.getRows());
		
		config.hideLabels();
		
		Pipe<RowSchema<M>> rowResults = new Pipe<RowSchema<M>>(config);
		ColumnsToRowsStage<M> ctr = new ColumnsToRowsStage( gm,
															colResults,
															rowResults);
		return rowResults;
	}


	
	
	
}
