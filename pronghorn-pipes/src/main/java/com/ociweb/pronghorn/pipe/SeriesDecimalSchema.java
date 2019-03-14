package com.ociweb.pronghorn.pipe;

import com.ociweb.pronghorn.pipe.token.TypeMask;

public class SeriesDecimalSchema extends MessageSchema<SeriesDecimalSchema> {
	
	protected SeriesDecimalSchema() {
		super(FieldReferenceOffsetManager.buildAggregateNumberBlockFrom(TypeMask.Decimal, "Series"));
	}

	public static final SeriesDecimalSchema instance = new SeriesDecimalSchema();

}
