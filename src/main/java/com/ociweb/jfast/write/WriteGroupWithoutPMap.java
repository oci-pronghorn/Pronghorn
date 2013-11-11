package com.ociweb.jfast.write;

import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.NullAdjuster;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.read.FASTException;

public class WriteGroupWithoutPMap implements WriteEntry {

	private final int id;
	private final Field nib; //never changes but is unique to this node
	
	public WriteGroupWithoutPMap(int id, Field[][] matrix) {
		this.id = id;
		assert(1==matrix.length); //no pmap so why should we have more than one?
		this.nib = matrix[0][0];
	}

	
	
	public int writeGroup(PrimitiveWriter writer, ValueDictionaryEntry entry,
							int id, FASTProvide provider) {
		
		nib.writer(writer, provider);
		return 1;
	}

	public int writeLong(PrimitiveWriter writer, ValueDictionaryEntry entry,
			int id, NullAdjuster nullOff, FASTProvide value) {
		throw new FASTException();
	}

	public int writeInt(PrimitiveWriter writer, ValueDictionaryEntry entry,
			int id, NullAdjuster nullOff, FASTProvide value) {
		throw new FASTException();
	}

	public int writeBytes(PrimitiveWriter writer, ValueDictionaryEntry entry,
			int id, NullAdjuster nullOff, FASTProvide value) {
		throw new FASTException();
	}

	public int writeDecimal(PrimitiveWriter writer, ValueDictionaryEntry entry,
			int id, NullAdjuster nullOff, FASTProvide value) {
		throw new FASTException();
	}

	public int writeASCII(PrimitiveWriter writer, ValueDictionaryEntry entry,
			int id, NullAdjuster nullOff, FASTProvide value) {
		throw new FASTException();
	}

	public int writeUTF8(PrimitiveWriter writer, ValueDictionaryEntry entry,
			int id, NullAdjuster nullOff, FASTProvide value) {
		throw new FASTException();
	}

	public int writeUnsignedLong(PrimitiveWriter writer,
			ValueDictionaryEntry entry, int id, NullAdjuster nullOff,
			FASTProvide value) {
		throw new FASTException();
	}

	public int writeUnsignedInt(PrimitiveWriter writer,
			ValueDictionaryEntry entry, int id, NullAdjuster nullOff,
			FASTProvide value) {
		throw new FASTException();
	}



}
