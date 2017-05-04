package com.ociweb.pronghorn.stage.math;

public class ColumnSchema<M extends MatrixSchema<M>> extends MatrixSchema<ColumnSchema<M>> {

	protected ColumnSchema(MatrixSchema<M> matrixSchema) {
		super(matrixSchema.getRows(), 1, matrixSchema.type, MatrixSchema.singleNumberBlockFrom(matrixSchema.type, matrixSchema.rows));	
	}

}
