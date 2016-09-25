package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.MessageSchema;

public class ColumnSchema<M extends MatrixSchema> extends MessageSchema {

	protected ColumnSchema(M matrixSchema) {
		super(matrixSchema.getColumnFrom());
	}

}
