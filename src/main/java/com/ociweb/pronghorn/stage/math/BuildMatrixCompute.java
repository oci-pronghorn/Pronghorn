package com.ociweb.pronghorn.stage.math;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.pipe.schema.generator.TemplateGenerator;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;
import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.route.SplitterStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class BuildMatrixCompute {

	public enum MatrixTypes {
		Integers(TypeMask.IntegerSigned) {
			public void computeColumn(int rows, Pipe<?> columnPipe, Pipe<?> rowPipe, Pipe<?> outputPipe) {
	
				int sum = 0; 
				while (--rows >= 0) {
					int colValue = Pipe.takeValue(columnPipe); //take each value from this column
					int matrixValue = Pipe.takeValue(rowPipe); //take each value from the same sized row
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
			public void computeColumn(int rows, Pipe<?> columnPipe, Pipe<?> rowPipe, Pipe<?> outputPipe) {
				float sum = 0; 
				while (--rows >= 0) {
					float colValue = Float.intBitsToFloat(Pipe.takeValue(columnPipe)); //take each value from this column
					float matrixValue = Float.intBitsToFloat(Pipe.takeValue(rowPipe)); //take each value from the same sized row
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
			public void computeColumn(int rows, Pipe<?> columnPipe, Pipe<?> rowPipe, Pipe<?> outputPipe) {
				
				int  sumExp   = 0;
				long sumValue = 0;
				
				while (--rows >= 0) {
					
					int  colExp   = Pipe.takeValue(columnPipe);
					long colValue = Pipe.takeLong(columnPipe); //take each value from this column
					
					int matrixExp   = Pipe.takeValue(rowPipe);
					long matrixValue = Pipe.takeLong(rowPipe); //take each value from the same sized row
					
					
					long prod = colValue*matrixValue;
					int  exp = colExp + matrixExp;
					
				//	System.out.println("A wrote "+prod+" "+exp);
					
					if (exp==sumExp) {
						
						sumValue+=prod;
						
					} else if (exp>sumExp){
						//exp is larger so we must convert 
						int dif = (exp-sumExp);
						
						long temp =	sumValue / (int)(Math.pow(10, dif));
						sumValue = prod+temp;
						sumExp = exp;
					} else {
						int dif = (sumExp-exp);
						
						long temp =	prod / (int)(Math.pow(10, dif));
						sumValue = sumValue+temp;
					}	
					
				}
				
			////	System.out.println("wrote "+sumValue+" "+sumExp);
				
				//sum now holds the new value to be sent out
				Pipe.addIntValue(sumExp, outputPipe);
				Pipe.addLongValue(sumValue, outputPipe); 
			}

			   double[] powd = new double[] {
					  1.0E-64,1.0E-63,1.0E-62,1.0E-61,1.0E-60,1.0E-59,1.0E-58,1.0E-57,1.0E-56,1.0E-55,1.0E-54,1.0E-53,1.0E-52,1.0E-51,1.0E-50,1.0E-49,1.0E-48,1.0E-47,1.0E-46,
					  1.0E-45,1.0E-44,1.0E-43,1.0E-42,1.0E-41,1.0E-40,1.0E-39,1.0E-38,1.0E-37,1.0E-36,1.0E-35,1.0E-34,1.0E-33,1.0E-32,1.0E-31,1.0E-30,1.0E-29,1.0E-28,1.0E-27,1.0E-26,1.0E-25,1.0E-24,1.0E-23,1.0E-22,
					  1.0E-21,1.0E-20,1.0E-19,1.0E-18,1.0E-17,1.0E-16,1.0E-15,1.0E-14,1.0E-13,1.0E-12,1.0E-11,1.0E-10,1.0E-9,1.0E-8,1.0E-7,1.0E-6,1.0E-5,1.0E-4,0.001,0.01,0.1,1.0,10.0,100.0,1000.0,10000.0,100000.0,1000000.0,
					  1.0E7,1.0E8,1.0E9,1.0E10,1.0E11,1.0E12,1.0E13,1.0E14,1.0E15,1.0E16,1.0E17,1.0E18,1.0E19,1.0E20,1.0E21,1.0E22,1.0E23,1.0E24,1.0E25,1.0E26,1.0E27,1.0E28,1.0E29,1.0E30,1.0E31,1.0E32,1.0E33,1.0E34,1.0E35,
					  1.0E36,1.0E37,1.0E38,1.0E39,1.0E40,1.0E41,1.0E42,1.0E43,1.0E44,1.0E45,1.0E46,1.0E47,1.0E48,1.0E49,1.0E50,1.0E51,1.0E52,1.0E53,1.0E54,1.0E55,1.0E56,1.0E57,1.0E58,1.0E59,1.0E60,1.0E61,1.0E62,1.0E63,1.0E64};

			  
			@Override
			public void addValue(double i, Pipe<?> right) {
				 
				int places = 4;
				long value = (long)Math.rint(i*powd[64+places]);
	
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

		public abstract int size();
	}
	
	
	public static MatrixSchema buildSchema(int rows, int columns, MatrixTypes type) {

		int fields = 1;
		int size = TypeMask.ringBufferFieldSize[type.typeMask];
		if (type.typeMask==TypeMask.Decimal) {
			size = 3;
			fields = 2;
		}
		
		int colLen = (fields*rows)+1+1;//rows(how long the col is) one for name one for fields
		
		int matLen = (fields*rows*columns)+1+1;
				
		int[]    matrixTokens=new int[matLen];
		String[] matrixNames=new String[matLen];
		long[]   matrixIds=new long[matLen];
		
		int[]    colTokens=new int[colLen];
		String[] colNames=new String[colLen];
		long[]   colIds=new long[colLen];
		
		
		colIds[0] = 10000;
		colNames[0] = "Column";
		colTokens[0] = TokenBuilder.buildToken(TypeMask.Group, 0, (size*rows)+1);
		
		if (type.typeMask==TypeMask.Decimal) {
			int m = 1;
			for (int i=1;i<=rows;i++) {
				colIds[m]=i;
				colNames[m]=Integer.toString(i);
				colTokens[m] = TokenBuilder.buildToken(TypeMask.Decimal, 0, i); 
				m++;
				colIds[m]=i;
				colNames[m]=Integer.toString(i);
				colTokens[m] = TokenBuilder.buildToken(TypeMask.LongSigned, 0, i);
				m++;
			}
		} else {
			for (int i=1;i<=rows;i++) {
				colIds[i]=i;
				colNames[i]=Integer.toString(i);
				colTokens[i] = TokenBuilder.buildToken(type.typeMask, 0, i); 
				
			}
		}
		colTokens[colTokens.length-1] = TokenBuilder.buildToken(TypeMask.Group, OperatorMask.Group_Bit_Close, (size*rows)+1);
		//last position is left as null and zero
		assert(colIds[colIds.length-1]==0);
		assert(colNames[colNames.length-1]==null);

		
		int points = rows*columns;
		matrixIds[0] = 10000;
		matrixNames[0] = "Matrix";
		matrixTokens[0] = TokenBuilder.buildToken(TypeMask.Group, 0, (size*points)+1); 
		if (type.typeMask==TypeMask.Decimal) {
			int m = 1;
			for (int i=1;i<=points;i++) {
				matrixIds[m]=i;
				matrixNames[m]=Integer.toString(i);
				matrixTokens[m] = TokenBuilder.buildToken(TypeMask.Decimal, 0, i); 
				m++;
				matrixIds[m]=i;
				matrixNames[m]=Integer.toString(i);
				matrixTokens[m] = TokenBuilder.buildToken(TypeMask.LongSigned, 0, i);
				m++;
			}
		} else {
			for (int i=1;i<=points;i++) {
				matrixIds[i]=i;
				matrixNames[i]=Integer.toString(i);
				matrixTokens[i] = TokenBuilder.buildToken(type.typeMask, 0, i);
				
			}
		}
		matrixTokens[matrixTokens.length-1] = TokenBuilder.buildToken(TypeMask.Group, OperatorMask.Group_Bit_Close, (size*points)+1);
		//last position is left as null and zero
		assert(matrixIds[matrixIds.length-1]==0);
		assert(matrixNames[matrixNames.length-1]==null);
				
		return new MatrixSchema(new FieldReferenceOffsetManager(matrixTokens, /*pramble*/ (short)0, matrixNames, matrixIds), 
				                new FieldReferenceOffsetManager(colTokens, /*pramble*/ (short)0, colNames, colIds),
				                rows, columns, type);
	}

	public static MatrixSchema buildResultSchema(MatrixSchema leftSchema, MatrixSchema rightSchema) {
		assert(leftSchema.type == rightSchema.type);
		return buildSchema(leftSchema.getRows(), rightSchema.getColumns(), leftSchema.type);
	}
	
	
	public static <M extends MatrixSchema, L extends MatrixSchema, R extends MatrixSchema>
	            void buildGraph(GraphManager gm, M resultSchema,  L leftSchema, R rightSchema, Pipe<L> leftInput, Pipe<R> rightInput, Pipe<M> result) {
		
		int i = resultSchema.getColumns();
		Pipe<L>[] splitterPipes = new Pipe[i];
		Pipe<ColumnSchema<R>>[] intputAsColumns = new Pipe[i];
		Pipe<ColumnSchema<M>>[] resultInColumns = new Pipe[i];
				
		PipeConfig<ColumnSchema<R>> rightColumnConfig = new PipeConfig<ColumnSchema<R>>(new ColumnSchema<R>(rightSchema),1);
		PipeConfig<ColumnSchema<M>> resultColumnConfig = new PipeConfig<ColumnSchema<M>>(new ColumnSchema<M>(resultSchema),1);
				
		while (--i>=0) {
			splitterPipes[i] = new Pipe<L>(leftInput.config().grow2x());
			intputAsColumns[i] = new Pipe<ColumnSchema<R>>(rightColumnConfig);			
			resultInColumns[i] =  new Pipe<ColumnSchema<M>>(resultColumnConfig);			
			new ColumnComputeStage(gm, intputAsColumns[i], splitterPipes[i], resultInColumns[i], resultSchema, leftSchema, rightSchema);
						
		}
		//split the left matrix into N column pipes.
		new ColumnsRouteStage(gm, rightSchema, rightInput, intputAsColumns);
		new SplitterStage<L>(gm, leftInput, splitterPipes); //duplicate the matrix once for each column.		
		new ColumnsJoinStage<M>(gm, resultSchema, resultInColumns, result);
			
		
	}


	
	
	
}
