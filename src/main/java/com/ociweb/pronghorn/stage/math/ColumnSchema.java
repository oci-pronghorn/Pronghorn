package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;

public class ColumnSchema<M extends MatrixSchema<M>> extends MatrixSchema<ColumnSchema<M>> {

	protected ColumnSchema(MatrixSchema<M> matrixSchema) {
		super(matrixSchema.getRows(), 1, matrixSchema.type, FieldReferenceOffsetManager.buildSingleNumberBlockFrom(matrixSchema.rows, matrixSchema.type.typeMask, "Matrix"));	
	}

}
