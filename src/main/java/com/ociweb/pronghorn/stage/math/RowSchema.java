package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;

public class RowSchema<M extends MatrixSchema<M>> extends MatrixSchema<RowSchema<M>> {

	public RowSchema(MatrixSchema<M> matrixSchema) {
		super(matrixSchema.getColumns(), 1, matrixSchema.type, FieldReferenceOffsetManager.buildSingleNumberBlockFrom(matrixSchema.columns, matrixSchema.type.typeMask, "Matrix"));
	}

}
