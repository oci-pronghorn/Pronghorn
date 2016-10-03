package com.ociweb.pronghorn.stage.math;

public class ColumnSchema<M extends MatrixSchema> extends MatrixSchema {

	protected ColumnSchema(M matrixSchema) {
		super(matrixSchema.getRows(), 1, matrixSchema.type, MatrixSchema.singleNumberBlockFrom(matrixSchema.type, matrixSchema.rows));	
	}

}
