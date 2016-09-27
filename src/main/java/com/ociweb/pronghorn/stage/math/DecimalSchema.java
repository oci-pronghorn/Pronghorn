package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.MessageSchema;

public class DecimalSchema<M extends MatrixSchema> extends MessageSchema {

	protected DecimalSchema(M matrixSchema) {
		super(matrixSchema.getDecimalFrom());
	}

}
