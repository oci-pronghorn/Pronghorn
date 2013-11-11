package com.ociweb.jfast.field.integer;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.read.FASTError;
import com.ociweb.jfast.read.FASTException;

public final class FieldIntConstantOptional extends Field {

	private final int id;
	//old next field
	private final ValueDictionaryEntry valueDictionaryEntry;
	private final int repeat;
	
	public FieldIntConstantOptional(int id, ValueDictionaryEntry valueDictionaryEntry) {
		
		if (valueDictionaryEntry.isNull) {
			throw new FASTException(FASTError.CONST_INIT);
		}
		this.id = id;
		//next assignment	
		this.valueDictionaryEntry = valueDictionaryEntry;
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
			
		//don't send if the bit is zero
		if (reader.popPMapBit()!=0) {
			visitor.accept(id, valueDictionaryEntry.intValue);
		}
		//end of reader
		
	}

	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		
		//if provider has value
		//must set 1 bit in writer if not leave it zero?
		
		//end of writer
		
	}
	
	public void reset() {
		//end of reset
	}

}
