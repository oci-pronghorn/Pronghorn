package com.ociweb.jfast.field.integer;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldIntNulls extends Field {

	private final int id;
	private final int repeat;
	
	public FieldIntNulls(int id) {
		
		this.id = id;
		this.repeat = 1;
		
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		if (reader.peekNull()) {
			reader.incPosition();
			visitor.accept(id);
		} else {		 
			visitor.accept(id, reader.readSignedIntegerNullable());
		}
		//end of reader
		
	}

	public void writer(PrimitiveWriter writer, FASTProvide provider) {
		if (provider.provideNull(id)) {
			writer.writeNull();
		} else {
			writer.writeSignedIntegerNullable(provider.provideInt(id));
		}
		//end of writer
	}
	
	public void reset() {
		//end of reset
	}

}
