package com.ociweb.jfast.field.binary;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.NullAdjuster;
import com.ociweb.jfast.ReadWriteEntry;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.read.FASTError;
import com.ociweb.jfast.read.FASTException;
import com.ociweb.jfast.read.FieldTypeReadWrite;
import com.ociweb.jfast.read.ReadEntry;
import com.ociweb.jfast.write.WriteEntry;

public final class FieldBytesConstant extends Field {

	private final int id;
	private final BytesShadow shadow;
	private final int repeat;
	
	public FieldBytesConstant(int id, ValueDictionaryEntry valueDictionaryEntry) {
		
		if (null==valueDictionaryEntry.bytesValue || 
			valueDictionaryEntry.isNull) {
			throw new FASTException(FASTError.MANDATORY_CONSTANT_NULL);
		}
		
		this.id = id;		
		this.shadow = new BytesShadow(valueDictionaryEntry.bytesValue,0,valueDictionaryEntry.bytesValue.length);
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		int i = repeat;
		while (--i>=0) {
			visitor.accept(id, shadow);		
			//end of reader
		}
	}

	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		//end of writer
	}
	
	public void reset() {
		//end of reset
	}

}
