package com.ociweb.jfast.write;

import com.ociweb.jfast.FASTProvide;
import com.ociweb.jfast.NullAdjuster;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.primitive.PrimitiveWriter;


public interface WriteEntry {
	
	int writeLong(PrimitiveWriter writer, ValueDictionaryEntry entry, int id, NullAdjuster nullOff, FASTProvide value);
	
	int writeUnsignedLong(PrimitiveWriter writer, ValueDictionaryEntry entry, int id, NullAdjuster nullOff, FASTProvide value);
	
	int writeInt(PrimitiveWriter writer, ValueDictionaryEntry entry, int id, NullAdjuster nullOff, FASTProvide value);
	
	int writeUnsignedInt(PrimitiveWriter writer, ValueDictionaryEntry entry, int id, NullAdjuster nullOff, FASTProvide value);
	
	int writeBytes(PrimitiveWriter writer, ValueDictionaryEntry entry, int id, NullAdjuster nullOff, FASTProvide value);
	
	int writeDecimal(PrimitiveWriter writer, ValueDictionaryEntry entry, int id, NullAdjuster nullOff, FASTProvide value);
	
	int writeASCII(PrimitiveWriter writer, ValueDictionaryEntry entry, int id, NullAdjuster nullOff, FASTProvide value);
	
	int writeUTF8(PrimitiveWriter writer, ValueDictionaryEntry entry, int id, NullAdjuster nullOff, FASTProvide value);
	
	int writeGroup(PrimitiveWriter writer, ValueDictionaryEntry entry, int id, FASTProvide value);
	
}
