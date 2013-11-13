package com.ociweb.jfast.field.decimal;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.field.util.ValueDictionaryEntry;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.read.FASTError;
import com.ociweb.jfast.read.FASTException;

public final class FieldDecimalConstant extends Field {
	
	private final int id;
	private final int exponent;
	private final long mantissa;
	private final int repeat;
	
	public FieldDecimalConstant(int id, ValueDictionaryEntry valueDictionaryEntry) {
		
		this.id = id;
		if (null==valueDictionaryEntry.decimalValue || 
				valueDictionaryEntry.isNull) {
				throw new FASTException(FASTError.MANDATORY_CONSTANT_NULL);
		}
		this.exponent = valueDictionaryEntry.decimalValue.exponent;
		this.mantissa = valueDictionaryEntry.decimalValue.mantissa;
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		int i = repeat;
		while (--i>=0) {
			visitor.accept(id, exponent, mantissa);
			//end of reader
		}
	}

	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		//end of writer
	}
	
	public void reset() {
		//end of reset
	}

}
