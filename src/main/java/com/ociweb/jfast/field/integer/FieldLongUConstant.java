package com.ociweb.jfast.field.integer;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.field.util.ValueDictionaryEntry;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.read.FASTError;
import com.ociweb.jfast.read.FASTException;

public final class FieldLongUConstant extends Field {

	private final int id;
	private final long longValue;
	private final int repeat;
	
	public FieldLongUConstant(int id, ValueDictionaryEntry valueDictionaryEntry) {
		
		if (valueDictionaryEntry.isNull) {
			throw new FASTException(FASTError.MANDATORY_CONSTANT_NULL);
	    }
		
		this.id = id;	
		this.longValue = valueDictionaryEntry.longValue;
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		visitor.accept(id, longValue);
		//end of reader
	}

	public void writer(PrimitiveWriter writer, FASTProvide provider) {
		//end of writer
	}
	
	public void reset() {
		//end of reset
	}

}
