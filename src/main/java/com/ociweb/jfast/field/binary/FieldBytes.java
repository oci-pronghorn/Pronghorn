package com.ociweb.jfast.field.binary;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldBytes extends Field {

	private final int id;
	private final int repeat;
	private final BytesShadow shadow = new BytesShadow();
	
	public FieldBytes(int id) {
		this.id = id;
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		int i = repeat;
		while (--i>=0) {	
			int arrayLength = reader.readUnsignedInteger();		
			int pos = reader.readBytesPosition(arrayLength);
			shadow.setBacking(reader.getBuffer(), pos, arrayLength); 	
			visitor.accept(id, shadow);
			
			//end of reader
		}
	}

	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		int i = repeat;
		while (--i>=0) {
			byte[] temp = provider.provideBytes(id);
			writer.writeUnsignedInteger(temp.length);
			writer.writeByteArrayData(temp);
			
			//end of writer
		}
		
	}
	
	public final void reset() {
		//end of reset
	}

}
