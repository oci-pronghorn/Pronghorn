package com.ociweb.jfast.field.integer;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public final class FieldInt extends Field {

	//TODO: need to use SAME field instance in each place its used to old dictionary value plus
	//TODO: we need some fields that share dictionary values to point to others to "share" those will take a hit!
	//will need 1 and 0 to share the field in this case we do not use the dictionary ever?
	
	
	private final int id;
	//this is final so hotspot may be unrolling the loop and eliminating this conditional
	private final int repeat;
	
	public FieldInt(int id) {
		
		this.id = id;
		this.repeat = 1;
		
	}

	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		int i = repeat;
		while (--i>=0) {		
			visitor.accept(id, reader.readSignedInteger());
			//end of reader
		}
		
	}

	
	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		int i = repeat;
		while (--i>=0) {
			writer.writeSignedInteger(provider.provideInt(id));
			//end of writer
		}
		
	}
	
	public void reset() {

		//end of reset
	}

}
