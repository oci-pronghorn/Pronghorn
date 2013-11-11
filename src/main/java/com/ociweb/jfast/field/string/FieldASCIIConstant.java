package com.ociweb.jfast.field.string;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldASCIIConstant extends Field {

	private final int id;
	//old next field
	private final CharSequence seq;
	private final int repeat;
	
	public FieldASCIIConstant(int id, ValueDictionaryEntry valueDictionaryEntry) {
		
		this.id = id;
		//next assignment		
		this.seq = valueDictionaryEntry.charValue;
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		
		//constants are never transmitted so there is never anything to read
		//however dictionaryEntry will hold the immutable value we must visit
		visitor.accept(id, seq);
		
		//end of reader
		
	}

	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		
		//constants are never provided at runtime so there is nothing to pull from
		//the provider and constants are never transmitted so we have nothing to write
		
		//end of writer
		
	}
	
	public void reset() {
		//end of reset
	}


}
