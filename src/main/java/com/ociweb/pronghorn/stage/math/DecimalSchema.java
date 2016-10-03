package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.stage.math.BuildMatrixCompute.MatrixTypes;

public class DecimalSchema<M extends MatrixSchema> extends MatrixSchema {

	public DecimalSchema(M matrixSchema) {
		super(matrixSchema.getColumns(), 1, MatrixTypes.Decimals, MatrixSchema.singleNumberBlockFrom(MatrixTypes.Decimals, matrixSchema.columns));
	}

}
