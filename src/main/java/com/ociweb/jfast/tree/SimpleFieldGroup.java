package com.ociweb.jfast.tree;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.field.Field;
import com.ociweb.jfast.field.integer.FieldInt;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class SimpleFieldGroup extends Field {

	//NOTE: must loop here because linked implementation caused stack overflow and this is quick with count down to zero.
	private final Field[] fields;
	//JULIA will in line all of these calls eliminating the virtual lookup completely
	
	public SimpleFieldGroup(Field[] fields) {
		this.fields = fields;
	}

	@Override
	public final void reader(PrimitiveReader reader, FASTAccept visitor) {
		int i = fields.length;
		while (--i>=0) {
			//visitor.accept(0, reader.readSignedInteger());
			//storage of entry is the second largest expense but local member storage only adds <5 ms
			//this virtual call adds about 17ns (by far biggest expense)
			fields[i].reader(reader, visitor);
		}

	}

	@Override
	public final void writer(PrimitiveWriter writer, FASTProvide provider) {
		int i = fields.length;
		while (--i>=0) {
			//writer.writeSignedInteger(provider.provideInt(0));
			//this virtual call appears to add about 7ns. TODO: if we could find a way to group them...
			fields[i].writer(writer, provider);
		}
	}

	@Override
	public final void reset() {
		int i = fields.length;
		while (--i>=0) {
			fields[i].reset();
		}
	}

}
