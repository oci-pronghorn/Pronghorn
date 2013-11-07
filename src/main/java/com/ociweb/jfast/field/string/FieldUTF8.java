package com.ociweb.jfast.field.string;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldUTF8 extends Field {

	private final int id;
	private final int repeat;
	
	public FieldUTF8(int id) {
		
		this.id = id;	
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
				
		int dataLength =reader.readUnsignedInteger();
		int pos = reader.readBytesPosition(dataLength);
		//NOTE: one of the few places where we create an object, It is not
		//easy to UTF-8 encode bytes and this appears to be the quickest way.
		visitor.accept(id, new String(reader.getBuffer(),pos,dataLength));
		
		//end of reader
		
	}

	public void writer(PrimitiveWriter writer, FASTProvide provider) {
		
		CharSequence seq = provider.provideCharSequence(id);
		writer.writeUnsignedInteger(seq.length());
		writer.writeByteArrayData(seq.toString().getBytes());
		
		//end of writer
		
	}
	
	public void reset() {
		//end of reset
	}

}
