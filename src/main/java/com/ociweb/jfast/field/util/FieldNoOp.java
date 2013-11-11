package com.ociweb.jfast.field.util;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FieldNoOp extends Field {

	//old next field
	public FieldNoOp(Field next) {
		//next assignment
	}
	
	@Override
	public void reader(PrimitiveReader reader, FASTAccept visitor) {
		//end of reader
	}

	@Override
	public void writer(PrimitiveWriter writer, FASTProvide provider) {
		//end of writer
	}

	@Override
	public void reset() {
		//end of reset
	}

}
