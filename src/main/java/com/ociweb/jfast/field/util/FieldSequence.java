package com.ociweb.jfast.field.util;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldSequence extends Field {

	
	private final int id;
	//old next field
	private final ValueDictionaryEntry valueDictionaryEntry;
	private final int repeat;
	
	public FieldSequence(int id, Field next, ValueDictionaryEntry valueDictionaryEntry) {
		
		this.id = id;
		//next assignment		
		this.valueDictionaryEntry = valueDictionaryEntry;
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
				
		//read group but I need nibble link chain
		
		
		
		
		//end of reader
		
	}

	public void writer(PrimitiveWriter writer, FASTProvide provider) {
		
		//write group needs nibble chain.
		
		
		
		
		//end of writer
		
	}
	
	public void reset() {
		valueDictionaryEntry.reset();
		//end of reset
	}

}
