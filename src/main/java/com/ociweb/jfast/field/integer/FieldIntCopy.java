package com.ociweb.jfast.field.integer;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.read.FASTError;
import com.ociweb.jfast.read.FASTException;

public final class FieldIntCopy extends Field {

	private final int id;
	private int intValue;
	private final int repeat;
	
	public FieldIntCopy(int id, ValueDictionaryEntry valueDictionaryEntry) {
		
		if (valueDictionaryEntry.isNull) {
			throw new FASTException(FASTError.CONST_INIT);
		}
		this.id = id;
		this.intValue = valueDictionaryEntry.intValue; ///TODO: what is the intial value?
		this.repeat = 1;
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		if (reader.popPMapBit()==0) {
			visitor.accept(id, intValue);//use last value
		} else {
			this.intValue = reader.readSignedInteger();
			visitor.accept(id, intValue);//use the new value and test it
		}
		//end of reader
		
	}

	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		
		
		int provided = provider.provideInt(id);
		
				
		if (provided==intValue) {
			//0 for pmap.
			writer.writePMapBit(0);
			
		} else {
			//1 for pmap.
			writer.writePMapBit(1);
			writer.writeSignedInteger(intValue = provider.provideInt(id));
		}
		
		
		//end of writer
		
		
	}
	
	public void reset() {
		//end of reset
	}

}
