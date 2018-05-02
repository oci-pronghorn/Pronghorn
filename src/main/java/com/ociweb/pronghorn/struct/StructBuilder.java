package com.ociweb.pronghorn.struct;

import java.util.Arrays;

import com.ociweb.pronghorn.util.CharSequenceToUTF8;
import com.ociweb.pronghorn.util.CharSequenceToUTF8Local;

public class StructBuilder {

	private final static int INIT_SIZE=16;
	
	private final StructRegistry typeData;
		
	private int fieldCount = 0;
	
	private byte[][] fieldNames;
	private StructTypes[] fieldTypes;
	private int[] fieldDims;
	private Object[] fieldAssoc;
	
	// type Store Registry,  StructRegistry
	public StructBuilder(StructRegistry typeData) {
		this.typeData = typeData;
		
		this.fieldNames = new byte[INIT_SIZE][];
		this.fieldTypes = new StructTypes[INIT_SIZE];
		this.fieldDims  = new int[INIT_SIZE];
		this.fieldAssoc  = new Object[INIT_SIZE];
		
	}
	
	public StructBuilder(StructRegistry typeData, StructBuilder template) {
		this.typeData = typeData;
		
		this.fieldNames = Arrays.copyOfRange(template.fieldNames, 0, template.fieldCount); 
		this.fieldTypes = Arrays.copyOfRange(template.fieldTypes, 0, template.fieldCount); 
		this.fieldDims  = Arrays.copyOfRange(template.fieldDims, 0, template.fieldCount); 
		this.fieldAssoc = Arrays.copyOfRange(template.fieldAssoc, 0, template.fieldCount); 
		
	}
	
	public static StructBuilder newStruct(StructRegistry typeData) {
		return new StructBuilder(typeData);
	}
	
	public static StructBuilder newStruct(StructRegistry typeData, StructBuilder template) {
		return new StructBuilder(typeData, template);
	}
	
	public StructBuilder removeLastNFields(int n) {
		fieldCount = Math.max(0, fieldCount-n);
		return this;
	}			
	
	public StructBuilder removeFieldWithName(CharSequence name) {
	
		CharSequenceToUTF8 c = CharSequenceToUTF8Local.get().convert(name);
		
		int i = fieldCount;
		while (--i>=0) {
			byte[] field = fieldNames[i];
			if (c.isEquals(field)) {	
				//move it all down
				fieldCount--;
				//was this field on the end?
				if (i<fieldCount) {
					//value at location i must be removed
					System.arraycopy(fieldNames, i+1, fieldNames, i, fieldCount-i);
					System.arraycopy(fieldTypes, i+1, fieldTypes, i, fieldCount-i);
					System.arraycopy(fieldDims,  i+1, fieldDims,  i, fieldCount-i);
				}					
				return this;
			}
		}
		throw new UnsupportedOperationException("Field "+name+" not found");
	}
	
	public StructBuilder addField(CharSequence fieldName, 
								  StructTypes fieldType) {
		return addField(fieldName, fieldType, 0, null);
	}
	
	public StructBuilder addField(CharSequence fieldName, 
            StructTypes fieldType, 
            int fieldDim) {
		return addField(fieldName,fieldType,fieldDim, null);
	}
	
	public StructBuilder addField(CharSequence fieldName, 
            StructTypes fieldType, 
            Object assoc) {
		return addField(fieldName, fieldType, 0, assoc);
	}

	public <T extends Enum<T>> StructBuilder addField(T fieldObject, StructTypes fieldType) {
		return addField(fieldObject.name(), fieldType, 0, fieldObject);
	}

	public <T extends Enum<T>> StructBuilder addField(T fieldObject, StructTypes fieldType, int fieldDim) {
		return addField(fieldObject.name(), fieldType, fieldDim, fieldObject);
	}
	
	public StructBuilder addField(CharSequence fieldName, 
			                 StructTypes fieldType, 
			                 int fieldDim, 
			                 Object assoc) {
		
		if (fieldCount == fieldTypes.length) {
			fieldNames = grow(fieldNames);
			fieldTypes = grow(fieldTypes);
			fieldDims = grow(fieldDims);
			fieldAssoc = grow(fieldAssoc);
		}
		
		fieldNames[fieldCount] = CharSequenceToUTF8Local.get().convert(fieldName).asBytes();
		fieldTypes[fieldCount] = fieldType;
		fieldDims[fieldCount] = fieldDim;
		fieldAssoc[fieldCount] = assoc;
		
		fieldCount++;
		return this;
	}
	
	private Object[] grow(Object[] source) {
		Object[] result = new Object[source.length*2];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private int[] grow(int[] source) {
		int[] result = new int[source.length*2];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private StructTypes[] grow(StructTypes[] source) {
		StructTypes[] result = new StructTypes[source.length*2];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private byte[][] grow(byte[][] source) {
		byte[][] result = new byte[source.length*2][];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	public int register() {
		return typeData.addStruct(
				null,
				Arrays.copyOfRange(fieldNames, 0, fieldCount), 
				Arrays.copyOfRange(fieldTypes, 0, fieldCount), 
				Arrays.copyOfRange(fieldDims, 0, fieldCount),
				Arrays.copyOfRange(fieldAssoc, 0, fieldCount)
				);		
	}
	
	public int register(Object associated) {
		return typeData.addStruct(
				associated,
				Arrays.copyOfRange(fieldNames, 0, fieldCount), 
				Arrays.copyOfRange(fieldTypes, 0, fieldCount), 
				Arrays.copyOfRange(fieldDims, 0, fieldCount),
				Arrays.copyOfRange(fieldAssoc, 0, fieldCount)
				);		
	}
	
}
