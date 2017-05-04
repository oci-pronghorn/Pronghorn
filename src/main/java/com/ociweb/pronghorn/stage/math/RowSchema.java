package com.ociweb.pronghorn.stage.math;

public class RowSchema<M extends MatrixSchema<M>> extends MatrixSchema<RowSchema<M>> {

	public RowSchema(MatrixSchema<M> matrixSchema) {
		super(matrixSchema.getColumns(), 1, matrixSchema.type, MatrixSchema.singleNumberBlockFrom(matrixSchema.type, matrixSchema.columns));
	}

}
