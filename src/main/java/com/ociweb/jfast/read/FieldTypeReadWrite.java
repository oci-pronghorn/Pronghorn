package com.ociweb.jfast.read;

import com.ociweb.jfast.Operator;
import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.field.util.ValueDictionaryEntry;
import com.ociweb.jfast.tree.Necessity;

public interface FieldTypeReadWrite {

	String name();
	Field newField(int ids, ValueDictionaryEntry valueDictionaryEntry, Operator operator, Necessity req);
}
