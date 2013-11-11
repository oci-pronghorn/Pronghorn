package com.ociweb.jfast.field.binary;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldBytesNulls extends Field {

	private final int id;
	private final int repeat;
	private final BytesShadow shadow = new BytesShadow();
		
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
				shadow.setBacking(reader.getBuffer(), pos, arrayLength);
				visitor.accept(id, shadow);
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
