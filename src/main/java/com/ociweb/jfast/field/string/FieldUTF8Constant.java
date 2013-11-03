package com.ociweb.jfast.field.string;

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

public final class FieldUTF8Constant extends Field {

	private final int id;
	private final CharSequence seq;
	private final int repeat;
	
	public FieldUTF8Constant(int id, ValueDictionaryEntry valueDictionaryEntry) {
		
		this.id = id;		
		this.seq = valueDictionaryEntry.charValue;
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
				
		visitor.accept(id, seq);
		//end of reader
		
	}

	public void writer(PrimitiveWriter writer, FASTProvide provider) {
		
		//end of writer
		
	}
	
	public void reset() {
		//end of reset
	}

}
