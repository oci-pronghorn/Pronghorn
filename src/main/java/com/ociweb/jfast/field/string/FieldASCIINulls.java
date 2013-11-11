package com.ociweb.jfast.field.string;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldASCIINulls extends Field {

	private final int id;
	//old next field
	private final CharSequenceShadow shadow = new CharSequenceShadow();
	private final int repeat;
	
	public FieldASCIINulls(int id, ValueDictionaryEntry valueDictionaryEntry) {
		
		this.id = id;
		//next assignment		
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		if (reader.peekNull()) {
			reader.incPosition();
			visitor.accept(id);
		} else {	
			reader.readASCII(shadow);
			visitor.accept(id, shadow);
		}
		//end of reader
		
	}

	public void writer(PrimitiveWriter writer, FASTProvide provider) {
		if (provider.provideNull(id)) {
			writer.writeNull(); 
		} else {
			writer.writeASCII(provider.provideCharSequence(id));
		}
		//end of writer
		
	}
	
	public void reset() {
		//end of reset
	}

}
