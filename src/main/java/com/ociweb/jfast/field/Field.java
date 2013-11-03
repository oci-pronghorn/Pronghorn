package com.ociweb.jfast.field;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.ReadWriteEntry;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public abstract class Field {

	public void reader(PrimitiveReader reader, FASTAccept visitor){};
	public void writer(PrimitiveWriter writer, FASTProvide provider){};
	public void reset(){};
	
}
