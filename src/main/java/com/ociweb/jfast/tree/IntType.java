package com.ociweb.jfast.tree;

import com.ociweb.jfast.read.FieldType;

public enum IntType {
	
	int32(FieldType.Int32),
	uInt32(FieldType.uInt32),
	int64(FieldType.Int64),
	uInt64(FieldType.uInt64);
	
	public final FieldType type;
		
	IntType(FieldType type) {
		this.type = type;
	}
	
}
