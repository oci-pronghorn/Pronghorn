package com.ociweb.jfast.field.binary;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.NullAdjuster;
import com.ociweb.jfast.ReadWriteEntry;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.read.FASTException;
import com.ociweb.jfast.read.FieldTypeReadWrite;
import com.ociweb.jfast.read.ReadEntry;
import com.ociweb.jfast.write.WriteEntry;

public final class FieldBytesDefault extends Field {

	private final int id;
	private final byte[] bytesValue;
	private final int repeat;
	
	public FieldBytesDefault(int id, ValueDictionaryEntry valueDictionaryEntry) {
		
		this.id = id;		
		this.bytesValue = valueDictionaryEntry.bytesValue;
		if (valueDictionaryEntry.isNull) {
			throw new FASTException();
		}
		this.repeat = 1;
	}
	
	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		int i = repeat;
		while (--i>=0) {
			//default - must never modify the valueDictionaryEntry
			if (reader.peekNull()) {
				reader.incPosition();
				//default - when null value is provided send the default
				visitor.accept(id, bytesValue, 0, bytesValue.length);
			} else {	
				
				int arrayLength = reader.readUnsignedIntegerNullable();
				int pos = reader.readBytesPosition(arrayLength);
				visitor.accept(id, reader.getBuffer(), pos, arrayLength);
			}
			//end of reader
		}
	}

	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		int i = repeat;
		while (--i>=0) {
			//default - write to stream but do not modify dictionary
			
			if (provider.provideNull(id)) {
				//do not write out to stream
				
			} else {
				//only write to stream
				
				byte[] byteValue = provider.provideBytes(id);
				writer.writeUnsignedIntegerNullable(byteValue.length);
				writer.writeByteArrayData(byteValue);	
				
			}
			//end of writer
		}
	}
	
	public void reset() {
		//end of reset
	}

}
