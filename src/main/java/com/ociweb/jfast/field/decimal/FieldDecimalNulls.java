package com.ociweb.jfast.field.decimal;

import com.ociweb.jfast.DecimalDTO;
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

public final class FieldDecimalNulls extends Field {
	
	private final int id;
	private final int repeat;

	DecimalDTO dto = new DecimalDTO();
	
	public FieldDecimalNulls(int id) {
		
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
				visitor.accept(id, reader.readSignedIntegerNullable(), reader.readSignedLong());
			}
			//end of reader
		}
		
	}

	public void writer(PrimitiveWriter writer, FASTProvide provider) {
		int i = repeat;
		while (--i>=0) {
			if (provider.provideNull(id)) {
				writer.writeNull();
			} else { 
				provider.provideDecimal(id, dto);
				writer.writeSignedIntegerNullable(dto.exponent);
				writer.writeSignedLong(dto.mantissa);	
			}
			//end of writer
		}
		
	}

	public void reset() {
		//end of reset
	}
	
}
