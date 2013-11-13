package com.ociweb.jfast.field.string;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.field.util.ValueDictionaryEntry;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldASCII extends Field {

	private final int id;
	//old next field
	private final CharSequenceShadow shadow = new CharSequenceShadow();
	private final int repeat;
	
	public FieldASCII(int id, ValueDictionaryEntry valueDictionaryEntry) {
		
		this.id = id;
		//next assignment		
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		
		reader.readASCII(shadow);   //this call only determines start and stop
		visitor.accept(id, shadow); //the copy of bytes happens here
		
		//end of reader
		
	}

	public void writer(PrimitiveWriter writer, FASTProvide provider) {
		
		writer.writeASCII(provider.provideCharSequence(id));
		
		//end of writer
		
	}

	public void reset() {
		//end of reset
	}

}
