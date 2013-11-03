package com.ociweb.jfast.field.integer;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.NullAdjuster;
import com.ociweb.jfast.ReadWriteEntry;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.read.FASTError;
import com.ociweb.jfast.read.FASTException;
import com.ociweb.jfast.read.FieldTypeReadWrite;
import com.ociweb.jfast.read.ReadEntry;
import com.ociweb.jfast.write.WriteEntry;

public final class FieldIntConstantMandatory extends Field {

	private final int id;
	//old next field
	private final ValueDictionaryEntry valueDictionaryEntry;
	private final int repeat;
	
	public FieldIntConstantMandatory(int id, ValueDictionaryEntry valueDictionaryEntry) {
		
		if (valueDictionaryEntry.isNull) {
			throw new FASTException(FASTError.MANDATORY_CONSTANT_NULL);
	    }
		
		this.id = id;
		//next assignment	
		this.valueDictionaryEntry = valueDictionaryEntry;
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
				
		visitor.accept(id, valueDictionaryEntry.intValue);
		//end of reader
		
	}

	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		
		//end of writer
		
	}
	
	public void reset() {
		//end of reset
	}

}
