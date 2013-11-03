package com.ociweb.jfast.field.integer;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.NullAdjuster;
import com.ociweb.jfast.ReadWriteEntry;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.read.FieldTypeReadWrite;
import com.ociweb.jfast.read.ReadEntry;
import com.ociweb.jfast.write.WriteEntry;

public final class FieldLongU extends Field {

	private final int id;
	private final int repeat;
	
	public FieldLongU(int id) {
		
		this.id = id;	
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		visitor.accept(id, reader.readUnsignedLong());
		//end of reader
	}

	public void writer(PrimitiveWriter writer, FASTProvide provider) {
		writer.writeUnsignedLong(provider.provideLong(id));
		//end of writer
	}
	
	public void reset() {
		//end of reset
	}

}
