package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.token.OperatorMask;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;
import com.ociweb.pronghorn.stage.math.BuildMatrixCompute.MatrixTypes;

public class MatrixSchema extends MessageSchema {

	public final int columnId = 0;
	public final int rowId = 0;
	public final int typeSize; //in ints
	public final MatrixTypes type;
	
	public final int rows;
	public final int columns;
	
	@Override
	public String toString() {
		return "rows:"+rows+" columns:"+columns+" of type "+type;
	}
	
	protected MatrixSchema(int rows, int columns, MatrixTypes type) {
		this(rows,columns,type,singleNumberBlockFrom(type, rows*columns));
		
	}
	
	protected MatrixSchema(int rows, int columns, MatrixTypes type, FieldReferenceOffsetManager from) {
		super(from);
									
		this.rows = rows;
		this.columns = columns;
		
		this.type = type;
		this.typeSize = type.size();

	}
	
	public static FieldReferenceOffsetManager singleNumberBlockFrom(MatrixTypes type, int points) {
		
		int fields = 1;
		int size = TypeMask.ringBufferFieldSize[type.typeMask];
		if (type.typeMask==TypeMask.Decimal) {
			size = 3;
			fields = 2;
		}		
		
		
		int matLen = (fields*points)+1+1;
		int[]    matrixTokens=new int[matLen];
		String[] matrixNames=new String[matLen];
		long[]   matrixIds=new long[matLen];
		matrixIds[0] = 10000;
		matrixNames[0] = "Matrix";
		
		int dataSize = (size*points)+1;
		
		matrixTokens[0] = TokenBuilder.buildToken(TypeMask.Group, 0, dataSize); 
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
		matrixTokens[matrixTokens.length-1] = TokenBuilder.buildToken(TypeMask.Group, OperatorMask.Group_Bit_Close, dataSize);
		//last position is left as null and zero
		assert(matrixIds[matrixIds.length-1]==0);
		assert(matrixNames[matrixNames.length-1]==null);
		FieldReferenceOffsetManager matFrom = new FieldReferenceOffsetManager(matrixTokens, /*pramble*/ (short)0, matrixNames, matrixIds);
		return matFrom;
	}
	
	public FieldReferenceOffsetManager getDecimalFrom() {
		return singleNumberBlockFrom(MatrixTypes.Decimals, rows*columns); 				
	}
	
	
	public int getRows() {
		return rows;
	}
	
	public int getColumns() {
		return columns;
	}


}
