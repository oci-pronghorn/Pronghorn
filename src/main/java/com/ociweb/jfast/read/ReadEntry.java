package com.ociweb.jfast.read;

import com.ociweb.jfast.FASTAccept;
import com.ociweb.jfast.NullAdjuster;
import com.ociweb.jfast.ValueDictionaryEntry;
import com.ociweb.jfast.primitive.PrimitiveReader;

public interface ReadEntry {

	void readLong(PrimitiveReader reader, int id, FASTAccept visitor, NullAdjuster nullAdjuster, ValueDictionaryEntry entry);

	void readUnsignedLong(PrimitiveReader reader, int id, FASTAccept visitor, NullAdjuster nullAdjuster, ValueDictionaryEntry entry);
	
	void readInt(PrimitiveReader reader, int id, FASTAccept visitor, NullAdjuster nullOff, ValueDictionaryEntry entry);

	void readUnsignedInt(PrimitiveReader reader, int id, FASTAccept visitor, NullAdjuster nullOff, ValueDictionaryEntry entry);
	
	void readBytes(PrimitiveReader reader, int id, FASTAccept visitor, NullAdjuster nullOff, ValueDictionaryEntry entry);
	
	void readCharsASCII(PrimitiveReader reader, int id, FASTAccept visitor, ValueDictionaryEntry entry);
	
	void readCharsUTF8(PrimitiveReader reader, int id, FASTAccept visitor, NullAdjuster nullOff, ValueDictionaryEntry entry);
	
	void readDecimal(PrimitiveReader reader, int id, FASTAccept visitor, NullAdjuster nullOff, ValueDictionaryEntry entry);
	
	void readGroup(PrimitiveReader reader, int id, FASTAccept visitor, ValueDictionaryEntry entry);

}
