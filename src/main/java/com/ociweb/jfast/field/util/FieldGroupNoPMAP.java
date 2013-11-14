package com.ociweb.jfast.field.util;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FieldGroupNoPMAP extends Field {

	//NOTE: must loop here because linked implementation caused stack overflow and this is quick with count down to zero.
	private final Field[] fields;
	//JULIA will in line all of these calls eliminating the virtual lookup completely
	
	public FieldGroupNoPMAP(Field[] fields) {
		this.fields = fields;
		
		//TODO: build special FASTOutput implementation
		//it does not flush any bytes unless its in flush mode when it takes another writer
		//all the bits back up forcing a grow of the internal buffer that we keep for next group instance.
		
		
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
		
		provider.beginGroup();
		
		//writer.pushPMap();
		
		//write all my child fields to my local group writer the parent group needs to finish 
		//writing its pmap first
		int i = fields.length;
		while (--i>=0) {
			//writer.writeSignedInteger(provider.provideInt(0));
			//this virtual call appears to add about 7ns. TODO: if we could find a way to group them...
			fields[i].writer(writer, provider);
		}
		//now write the pmap from group writer into writer
		//TODO:write
		//now write the byte data into the writer into writer
		
		//writer.writePMap();
	//	groupWriter
		
		provider.endGroup();
		
	}

	@Override
	public final void reset() {
		int i = fields.length;
		while (--i>=0) {
			fields[i].reset();
		}
	}

}
