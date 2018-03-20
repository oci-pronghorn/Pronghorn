package com.ociweb.pronghorn.struct;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class BStructSchema {
	
	//selected headers, route params, plus json payloads makes up a fixed record structure
	private int structCount = 0;
	
	private TrieParser[]     fields      = new TrieParser[4]; //grow as needed for fields
	private byte[][][]       fieldNames  = new byte[4][][];
	private BStructTypes[][] fieldTypes  = new BStructTypes[4][];
	private int[][]          fieldDims   = new int[4][];
	private Object[][]       fieldLocals = new Object[4][];

	
	//TODO: future feature
	//private Class<Enum<?>>[][] fieldOptionalEnum = new Class[4][]; //TODO: add method to set this on a field.

	
	private int              maxDims    = 0;
	
	private static final int STRUCT_BITS   = 20;
	private static final int STRUCT_MAX    = 1<<STRUCT_BITS;
	private static final int STRUCT_MASK   = STRUCT_MAX-1;
	private static final int STURCT_OFFSET = 32;
	private static final int IS_STRUCT_BIT = 1<<31;
	
	private static final int FIELD_BITS    = 20;
	private static final int FIELD_MAX     = 1<<FIELD_BITS;
	private static final int FIELD_MASK    = FIELD_MAX-1;
		
	
	
	public BStructSchema merge(BStructSchema source) {
		
		BStructSchema result = new BStructSchema();
		
		for(int i=0; i<structCount; i++) {			
			maxDims(fieldDims[i]);
			result.addStruct(fieldNames[i], fieldTypes[i], fieldDims[i]);			
		}
		for(int i=0; i<source.structCount; i++) {			
			result.addStruct(source.fieldNames[i], source.fieldTypes[i], source.fieldDims[i]);			
		}	
		
		return result;
	}
	
	public String toString() {
		return asSource(new StringBuilder()).toString();	
	}

	public <A extends Appendable> A asSource(A target) {
		
		try {
			target.append(BStructSchema.class.getSimpleName());
			target.append(" bss = new ").append(BStructSchema.class.getSimpleName()).append("();\n");
			
			String methodName = null;
			Method[] m = BStructSchema.class.getMethods();
			int i = m.length;
			while (--i>=0) {
				Class<?>[] parameterTypes = m[i].getParameterTypes();
				if (3 == parameterTypes.length 
						
					&& parameterTypes[0].getComponentType().isArray()
					&& parameterTypes[0].getComponentType() == String.class
					&& parameterTypes[1].getComponentType().isArray()
					&& parameterTypes[1].getComponentType() == BStructTypes.class
					&& parameterTypes[2].getComponentType().isArray()
					&& parameterTypes[2].getComponentType() == Integer.TYPE
				 ) {					
					methodName = m[i].getName();					
				}
			}
			
			for(int j = 0; j<structCount; j++) {
				target.append("bss.").append(methodName).append("(");
				target.append("new String[]{");
				
				byte[][] names = fieldNames[j];				
				for(int n=1; n<names.length; n++) {					
					if (n==0) {
						target.append("\"");
					} else {
						target.append(",\"");
					}
					Appendables.appendUTF8(target, names[n], 0, names[n].length, Integer.MAX_VALUE);
					target.append("\"");					
				}
				target.append("}, new ").append(BStructTypes.class.getName()).append("[]{");
				
				
				BStructTypes[] type = fieldTypes[j];				
				for(int t=1; t<type.length; t++) {					
					if (t!=0) {
						target.append(",");
					}
					target.append(BStructTypes.class.getName()).append('.');
					target.append(type[t].name());			
				}
				target.append("}, new int[]{");
				
				
				int[] dims = fieldDims[j];				
				for(int t=1; t<dims.length; t++) {					
					if (t!=0) {
						target.append(",");
					}
					Appendables.appendValue(target, dims[t]);
				}
				target.append("});\n");
				
			}
			
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		
		return target;
	}
	

//TODO: future feature, enum fields 
//	@SuppressWarnings("unchecked")
//	public <E extends Enum<E>> Enum<E> enumField(long id, int ordinal) {
//		
//		return (Enum<E>) fieldOptionalEnum
//				                          [STRUCT_MASK&(int)(id>>>STURCT_OFFSET)]
//				                          [FIELD_MASK&(int)id]
//				                        		  .getEnumConstants()[ordinal];
//	
//		
//		
//	}
	
	
	/**
	 * Add new Structure to the schema
	 * @param fieldNames - name for each field
	 * @param fieldTypes - type for each field
	 * @param fieldDims - dominations for this field, should be 0 for most cases of simple data
	 * @return the array of field identifiers in the same order as defined
	 */
	public int addStruct(byte[][] fieldNames, 
			                BStructTypes[] fieldTypes, //all fields are precede by array count byte
			                int[] fieldDims //Dimensionality, should be 0 for simple objects.
						   ) {
		
		assert(fieldNames.length == fieldTypes.length);
		assert(fieldNames.length == fieldDims.length);
		maxDims(fieldDims);
		int idx = structCount++;
		grow(structCount);
		
		int n = fieldNames.length;
		TrieParser fieldParser = new TrieParser(n*20,2,true,false);
		while (--n>=0) {			
			assert(isNotAlreadyDefined(fieldParser, fieldNames[n]));			
			fieldParser.setValue(fieldNames[n], n);
		}
		this.fields[idx] = fieldParser;
		
		this.fieldNames[idx] = fieldNames;
		this.fieldTypes[idx] = fieldTypes;
		this.fieldDims[idx] = fieldDims;
				
		return idx|IS_STRUCT_BIT;
	}
		
	
	private static boolean isNotAlreadyDefined(TrieParser fieldParser, byte[] value) {
		//we are trusting escape analysis to not create GC here.
		return -1 == fieldIdLookup(fieldParser, value, 0, value.length, Integer.MAX_VALUE);
	}

	private static long fieldIdLookup(TrieParser fieldParser, byte[] value, int valuePos, int valueLen, int mask) {
		return TrieParserReader.query(new TrieParserReader(true), fieldParser, value, valuePos, valueLen, mask);
	}

	public void growStruct(int structId,
						   byte[] fieldName,
						   BStructTypes fieldType,
						   int fieldDim) {
		assert((IS_STRUCT_BIT&structId)!=0) : "must be valid struct";
		int idx = STRUCT_MASK & structId;
		
		//add text lookup
		assert(isNotAlreadyDefined(this.fields[idx], fieldName));
		this.fields[idx].setValue(fieldName, this.fieldNames.length);
		
		//grow all the arrays with new value
		this.fieldNames[idx] = grow(this.fieldNames[idx], fieldName);
		this.fieldTypes[idx] = grow(this.fieldTypes[idx], fieldType);
		this.fieldDims[idx] = grow(this.fieldDims[idx], fieldDim);
		
	}
	
	public void modifyStruct(int structId,
						   byte[] fieldName, int fieldPos, int fieldLen,
						   BStructTypes fieldType,
						   int fieldDim) {
		assert((IS_STRUCT_BIT&structId)!=0) : "must be valid struct";
		int idx = STRUCT_MASK & structId;
		int fieldIdx = (int)fieldIdLookup(this.fields[idx], fieldName, fieldPos, fieldLen, Integer.MAX_VALUE);
		if (-1 == fieldIdx) {
			//only creating copy here because we know it will be held for the run.
			growStruct(structId, Arrays.copyOfRange(fieldName,fieldPos,fieldLen), fieldType, fieldDim);
		} else {
			//////////
			//modify an existing field, we have new data to apply
			//////////
			//use the newest defined type
			this.fieldTypes[idx][fieldIdx] = fieldType;		
			//keep largest dim value
			this.fieldDims[idx][fieldIdx] = Math.max(this.fieldDims[idx][fieldIdx], fieldDim);
		}
	}
			 
	
	

	private int[] grow(int[] source, int newValue) {
		int[] results = new int[source.length+1];
		System.arraycopy(source, 0, results, 0, source.length);
		results[source.length] = newValue;
		return results;
	}

	private BStructTypes[] grow(BStructTypes[] source, BStructTypes newValue) {
		BStructTypes[] results = new BStructTypes[source.length+1];
		System.arraycopy(source, 0, results, 0, source.length);
		results[source.length] = newValue;
		return results;
	}

	private byte[][] grow(byte[][] source, byte[] newValue) {
		byte[][] results = new byte[source.length+1][];
		System.arraycopy(source, 0, results, 0, source.length);
		results[source.length] = newValue;
		return results;
	}

	public long[] newFieldIds(int structId) {
		assert((IS_STRUCT_BIT&structId)!=0) : "must be valid struct";
		int idx = STRUCT_MASK & structId;
				
		int c = fieldNames.length;
		assert(c<=FIELD_MASK) : "too many fields";
		
		long[] fieldIds = new long[c];
		long base = ((long)idx)<<STURCT_OFFSET;
		while (--c>=0) {
			fieldIds[c] =  base | (long)c;
		}
		return fieldIds;
	}
	
	public void setAssociatedObject(long id, Object localObject) {
		this.fieldLocals[STRUCT_MASK&(int)(id>>>STURCT_OFFSET)][FIELD_MASK&(int)id] = localObject;
	}
	
	public <T extends Object> T getAssociatedObject(long id) {
		return (T) this.fieldLocals[STRUCT_MASK&(int)(id>>>STURCT_OFFSET)][FIELD_MASK&(int)id];
	}
	
	private void maxDims(int[] fieldDims) {
		int x = fieldDims.length;
		while (--x>=0) {
			if (fieldDims[x]>maxDims) {
				maxDims = fieldDims[x];
			}	
		}
		
		
	}

	public int dims(long id) {
		return fieldDims[STRUCT_MASK&(int)(id>>>STURCT_OFFSET)][FIELD_MASK&(int)id];
	}
	
	public BStructTypes fieldType(long id) {
		return fieldTypes[STRUCT_MASK&(int)(id>>>STURCT_OFFSET)][FIELD_MASK&(int)id];
	}
	
	public byte[] fieldName(long id) {
		return fieldNames[STRUCT_MASK&(int)(id>>>STURCT_OFFSET)][FIELD_MASK&(int)id];
	}
	    	
	public long fieldLookup(TrieParserReader reader, CharSequence sequence, int struct) {
		return TrieParserReader.query(reader, fields[struct], sequence);
	}
	
	public long fieldLookup(TrieParserReader reader, byte[] source, int pos, int len, int mask, int struct) {
		return TrieParserReader.query(reader, fields[struct], source, pos, len, mask);
	}
	
	private void grow(int records) {
		if (records>fields.length) {
			int newSize = records*2;
			
			fields      = grow(newSize, fields);
			fieldNames  = grow(newSize, fieldNames);
			fieldTypes  = grow(newSize, fieldTypes);
			fieldDims   = grow(newSize, fieldDims);
			fieldLocals = grow(newSize, fieldLocals);
						
		}
	}


	private static BStructTypes[][] grow(int newSize, BStructTypes[][] source) {
		BStructTypes[][] result = new BStructTypes[newSize][];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}


	private static byte[][][] grow(int newSize, byte[][][] source) {
		byte[][][] result = new byte[newSize][][];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private static TrieParser[] grow(int newSize, TrieParser[] source) {
		TrieParser[] result = new TrieParser[newSize];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private static Object[][] grow(int newSize, Object[][] source) {
		Object[][] result = new Object[newSize][];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}
	
	private static int[][] grow(int newSize, int[][] source) {
		int[][] result = new int[newSize][];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}
	
	private static long[][] grow(int newSize, long[][] source) {
		long[][] result = new long[newSize][];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}
	
	public int maxDim() {
		return maxDims;
	}

	public <T> void visit(DataInputBlobReader<?> reader, 
			              Class<T> attachedInstanceOf, 
			              BStructFieldVisitor<T> visitor) {
				
		int structId = DataInputBlobReader.getStructType(reader);
		if (structId>0) {
			Object[] locals = this.fieldLocals[structId];
			for(int i = 0; i<locals.length; i++) {
				if (attachedInstanceOf.isInstance(locals[i])) {
					DataInputBlobReader.position(reader, DataInputBlobReader.readFromLastInt(reader, i));								
					visitor.read((T) locals[i], reader);
				}
			}
		}
	}
	
}