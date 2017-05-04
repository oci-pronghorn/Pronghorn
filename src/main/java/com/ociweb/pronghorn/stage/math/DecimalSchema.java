package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.stage.math.BuildMatrixCompute.MatrixTypes;

public class DecimalSchema<M extends MatrixSchema<M>> extends MatrixSchema<DecimalSchema<M>> {

	public DecimalSchema(MatrixSchema<M> matrixSchema) {
		super(matrixSchema.getColumns(), 1, MatrixTypes.Decimals, MatrixSchema.singleNumberBlockFrom(MatrixTypes.Decimals, matrixSchema.columns));
	}

}
