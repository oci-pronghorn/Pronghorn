package com.ociweb.jfast.field.integer;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldLong extends Field {

	private final int id;
	private final int repeat;
	
	public FieldLong(int id) {
		
		this.id = id;	
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		
		visitor.accept(id,  reader.readSignedLong());
		//end of reader
		
	}

	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		
		writer.writeSignedLong(provider.provideLong(id));
		//end of writer
		
	}
	
	public void reset() {
		//end of reset
	}

}
