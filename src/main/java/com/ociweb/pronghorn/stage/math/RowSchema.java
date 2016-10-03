package com.ociweb.pronghorn.stage.math;

public class RowSchema<M extends MatrixSchema> extends MatrixSchema {

	public RowSchema(M matrixSchema) {
		super(matrixSchema.getColumns(), 1, matrixSchema.type, MatrixSchema.singleNumberBlockFrom(matrixSchema.type, matrixSchema.columns));
	}

}
