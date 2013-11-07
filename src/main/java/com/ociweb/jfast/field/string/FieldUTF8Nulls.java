package com.ociweb.jfast.field.string;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldUTF8Nulls extends Field {

	private final int id;
	private final int repeat;
	
	public FieldUTF8Nulls(int id) {
		
		this.id = id;
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		if (reader.peekNull()) {
			reader.incPosition();
			visitor.accept(id);
		} else {		
			int dataLength = reader.readUnsignedIntegerNullable();
			int pos = reader.readBytesPosition(dataLength);
			visitor.accept(id, new String(reader.getBuffer(),pos,dataLength));
		}
		//end of reader
		
	}

	public void writer(PrimitiveWriter writer, FASTProvide provider) {
		if (provider.provideNull(id)) {
			writer.writeNull();
		} else {
			CharSequence seq = provider.provideCharSequence(id);
			writer.writeUnsignedIntegerNullable(seq.length());
			writer.writeByteArrayData(seq.toString().getBytes());
		}
		
		//end of writer
		
	}
	
	public void reset() {
		//end of reset
	}

}
