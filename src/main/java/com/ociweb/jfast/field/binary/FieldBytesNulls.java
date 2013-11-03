package com.ociweb.jfast.field.binary;

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

public final class FieldBytesNulls extends Field {

	private final int id;
	private final int repeat;
		
	public FieldBytesNulls(int id) {
		
		this.id = id;	
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		int i = repeat;
		while (--i>=0) {
			if (reader.peekNull()) {
				reader.incPosition();
				visitor.accept(id);
			} else {	
				int arrayLength = reader.readUnsignedIntegerNullable();
				int pos = reader.readBytesPosition(arrayLength);
				visitor.accept(id,reader.getBuffer(),pos,arrayLength);
			}
			//end of reader
		}
	}

	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		int i = repeat;
		while (--i>=0) {
			if (provider.provideNull(id)) {
				writer.writeNull();
			} else {
				byte[] bytesValue = provider.provideBytes(id); //TODO: may be better way to do this?
				writer.writeUnsignedIntegerNullable(bytesValue.length);
				writer.writeByteArrayData(bytesValue);	
			}
			//end of writer
		}
	}
	
	public void reset() {
		//end of reset
	}

}
