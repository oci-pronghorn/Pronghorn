package com.ociweb.jfast.field.binary;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;


//NoOpp field that copies data from one field dictionary entry to another
//this is rarely used except when a second field has an explicit dictionary 
//reference to use the same "previous value" as another field.
//there is a performance hit for using this type of setup but its rare and
//optimizing for the most common case is more important
public class ShareBytes extends Field {

	private final Field sourceField;
	private final Field targetField;
	
	public ShareBytes(Field sourceField, Field targetField) {
		this.sourceField = sourceField;
		this.targetField = targetField;
	}
	
	@Override
	public void reader(PrimitiveReader reader, FASTAccept visitor) {
		//TODO: copy data from source into target but how?
		// 1. add memo method onto field that will produce the Entry again.  then also take memo method (possible)
		// 2. add all the get and sets into field then call all of them every time (ugly)
		// 3. add new type specific interface on to fields that has specific copy. (best?)
		
	}

	@Override
	public void writer(PrimitiveWriter writer, FASTProvide provider) {

	}

	@Override
	public void reset() {

	}

}
