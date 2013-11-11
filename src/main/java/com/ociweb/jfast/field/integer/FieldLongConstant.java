package com.ociweb.jfast.field.integer;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.read.FASTError;
import com.ociweb.jfast.read.FASTException;

public final class FieldLongConstant extends Field {

	private final int id;
	private final long longValue;
	private final int repeat;
	
	public FieldLongConstant(int id, ValueDictionaryEntry valueDictionaryEntry) {
		
		this.id = id;		
		this.longValue = valueDictionaryEntry.longValue;
		this.repeat = 1;
		
		if (valueDictionaryEntry.isNull) {
			throw new FASTException(FASTError.MANDATORY_CONSTANT_NULL);
	    }
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		visitor.accept(id, longValue);
		//end of reader
	}

	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		//end of writer
	}
	
	public void reset() {
		//end of reset
	}

}
