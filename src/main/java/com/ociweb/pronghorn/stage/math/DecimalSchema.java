package com.ociweb.pronghorn.stage.math;

import com.ociweb.pronghorn.pipe.MessageSchema;

public class DecimalSchema<M extends MatrixSchema> extends MessageSchema {

	public DecimalSchema(M matrixSchema) {
		super(matrixSchema.getDecimalFrom());
	}

}
