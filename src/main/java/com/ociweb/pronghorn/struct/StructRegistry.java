package com.ociweb.pronghorn.struct;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONRequired;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.util.hash.IntHashTable;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ArrayGrow;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.TrieParserReaderLocal;

public class StructRegistry { //prong struct store  
	
	private final static Logger logger = LoggerFactory.getLogger(StructRegistry.class);
	
	//selected headers, route params, plus json payloads makes up a fixed record structure
	private int structCount = 0;
	
	//field names supported here
	private TrieParser[]     fields             = new TrieParser[4]; //grow as needed for fields
	private byte[][][]       fieldNames         = new byte[4][][];
	//type of the field data, its dims and how it can be parsed.
	private StructType[][]   fieldTypes         = new StructType[4][];
	private int[][]          fieldDims          = new int[4][];
	private Object[][]       fieldLocals        = new Object[4][];
	private Object[][]       fieldValidators    = new Object[4][];
	private boolean[][]      fieldRequired      = new boolean[4][];	
	private IntHashTable[]   fieldAttachedIndex = new IntHashTable[4];
	private Object[]         structLocals       = new Object[4];
	
	private IntHashTable structTable = new IntHashTable(3);
	
	
	//TODO: future feature
	//private Class<Enum<?>>[][] fieldOptionalEnum = new Class[4][]; //TODO: add method to set this on a field.

	
	private int              maxDims    = 0;
	
	private static final int STRUCT_BITS   = 20;
	private static final int STRUCT_MAX    = 1<<STRUCT_BITS;
	private static final int STRUCT_MASK   = STRUCT_MAX-1;
	public static final int STRUCT_OFFSET = 32;
	public static final int IS_STRUCT_BIT = 1<<30;
	
	private static final int FIELD_BITS    = 20;
	private static final int FIELD_MAX     = 1<<FIELD_BITS;
	public static final int FIELD_MASK    = FIELD_MAX-1;
		
	
	public void registerStructAssociation(int structId, Object associatedObject) {

		int key = associatedObject.hashCode();
	
		assert(!IntHashTable.hasItem(structTable, key)) : "These objects are too similar or was attached twice, Hash must be unique. Choose different objects";
		if (IntHashTable.hasItem(structTable, key)) {
			logger.warn("Unable to add object {} as an association, Another object with an identical Hash is already held. Try a different object.", associatedObject);		
			return;
		}
		if (!IntHashTable.setItem(structTable, key, structId)) {
			structTable = IntHashTable.doubleSize(structTable);			
			if (!IntHashTable.setItem(structTable, key, structId)) {
				throw new RuntimeException("internal error");
			};
		}		
	}
	
	
	public StructRegistry merge(StructRegistry source) {
		
		StructRegistry result = new StructRegistry();
		
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
			target.append(StructRegistry.class.getSimpleName());
			target.append(" bss = new ").append(StructRegistry.class.getSimpleName()).append("();\n");
			
			String methodName = null;
			Method[] m = StructRegistry.class.getMethods();
			int i = m.length;
			while (--i>=0) {
				Class<?>[] parameterTypes = m[i].getParameterTypes();
				if (3 == parameterTypes.length 
						
					&& parameterTypes[0].getComponentType().isArray()
					&& parameterTypes[0].getComponentType() == String.class
					&& parameterTypes[1].getComponentType().isArray()
					&& parameterTypes[1].getComponentType() == StructType.class
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
				target.append("}, new ").append(StructType.class.getName()).append("[]{");
				
				
				StructType[] type = fieldTypes[j];				
				for(int t=1; t<type.length; t++) {					
					if (t!=0) {
						target.append(",");
					}
					target.append(StructType.class.getName()).append('.');
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
	
	
	public int addStruct() {
		return addStruct(new byte[][] {}, new StructType[] {}, new int[] {});
	}
	
	public int addStruct(byte[][] fieldNames, 
            StructType[] fieldTypes //all fields are precede by array count byte
            
			) {
		return addStruct(fieldNames, fieldTypes, null, null);
	}
	
	public int addStruct(byte[][] fieldNames, 
            StructType[] fieldTypes, //all fields are precede by array count byte
            int[] fieldDims //Dimensionality, should be 0 for simple objects.
			) {
		return addStruct(fieldNames, fieldTypes, fieldDims, null);
	}
	
	public int addStruct(
            byte[][] fieldNames, 
            StructType[] fieldTypes, //all fields are precede by array count byte
            int[] fieldDim, //Dimensionality, should be 0 for simple objects.
            Object[] fieldAssoc
			) {
		return addStruct(null,fieldNames,fieldTypes,fieldDim,fieldAssoc);
	}
	
	/**
	 * Add new Structure to the schema
	 * @param associatedObject - reference object
	 * @param fieldNames - name for each field
	 * @param fieldTypes - type for each field
	 * @param fieldDim - dominations for this field, should be 0 for most cases of simple data
	 * @return the array of field identifiers in the same order as defined
	 */
	public int addStruct(Object associatedObject,
			             byte[][] fieldNames, 
			             StructType[] fieldTypes, //all fields are precede by array count byte
			             int[] fieldDim, //Dimensionality, should be 0 for simple objects.
			             Object[] fieldAssoc
			) {
		return addStruct(associatedObject, fieldNames, fieldTypes, fieldDim, fieldAssoc, null, null);	
	}
	/**
	 * Add new Structure to the schema
	 * @param associatedObject - reference object
	 * @param fieldNames - name for each field
	 * @param fieldTypes - type for each field
	 * @param fieldDim - dominations for this field, should be 0 for most cases of simple data
	 * @return the array of field identifiers in the same order as defined
	 */
	public int addStruct(Object associatedObject,
			             byte[][] fieldNames, 
			             StructType[] fieldTypes, //all fields are precede by array count byte
			             int[] fieldDim, //Dimensionality, should be 0 for simple objects.
			             Object[] fieldAssoc,
			             JSONRequired[] fieldRequired,
			             Object[] fieldValidators			             
			) {
		
		assert(fieldNames.length == fieldTypes.length);
		if (null!=fieldDim) {
			assert(fieldNames.length == fieldDim.length);
			maxDims(fieldDim);
		}	
		int structIdx = structCount++;
		grow(structCount);
		
		int n = fieldNames.length;
		boolean skipDeepChecks = false;
		boolean supportsExtraction = false;
		boolean ignoreCase = true;
		TrieParser fieldParser = new TrieParser(n*20,4,skipDeepChecks,supportsExtraction,ignoreCase);
		long base = ((long)(IS_STRUCT_BIT|(STRUCT_MASK & structIdx)))<<STRUCT_OFFSET;
		while (--n>=0) {			
			assert(isNotAlreadyDefined(fieldParser, fieldNames[n]));			
			fieldParser.setValue(fieldNames[n], base | n); //bad value..
		}
		this.fields[structIdx] = fieldParser;
		
		this.fieldNames[structIdx] = fieldNames;
		this.fieldTypes[structIdx] = fieldTypes;
		this.fieldDims[structIdx] = null==fieldDim?new int[fieldTypes.length]:fieldDim;
		this.fieldLocals[structIdx] = new Object[fieldNames.length];
		this.fieldValidators[structIdx] = new Object[fieldNames.length];
		this.fieldRequired[structIdx] = new boolean[fieldNames.length];
		
		//added lots of extra space since many hash values are the same at the low digits
		this.fieldAttachedIndex[structIdx] = new IntHashTable(5+IntHashTable.computeBits(Math.max(fieldNames.length,8)));
				
		if (null!=fieldAssoc) {
			int fieldId = fieldAssoc.length;
			while (--fieldId >= 0) {
				if (null!=fieldAssoc[fieldId]) {
					setAssoc(fieldAssoc[fieldId], structIdx, fieldId);
				}
			}
		}
		
		if (null!=fieldValidators) {			
			int fieldId = fieldValidators.length;
			while (--fieldId >= 0) {
				if (null!=fieldValidators[fieldId]) {					
					setValidator(fieldRequired[fieldId], fieldValidators[fieldId], structIdx, fieldId);
				}
			}
		}
		
		int resultStructId = structIdx|IS_STRUCT_BIT;
		
		if (null!=associatedObject) {
			registerStructAssociation(resultStructId, associatedObject);
			structLocals[structIdx] = associatedObject;
		}
		
		return resultStructId;
	}
		
	
	private static boolean isNotAlreadyDefined(TrieParser fieldParser, byte[] raw) {
		long val = -1;
		boolean ok = -1 == (val=fieldIdLookup(fieldParser, raw, 0, raw.length, Integer.MAX_VALUE));
		if (!ok) {
			System.err.println("Already have text "+new String(raw)+" associated with "+val);
		}
		
		return ok;
	}

	private static long fieldIdLookup(TrieParser fieldParser, byte[] value, int valuePos, int valueLen, int mask) {
		//we are trusting escape analysis to not create GC here.
		return TrieParserReader.query(new TrieParserReader(true), fieldParser, value, valuePos, valueLen, mask);
	}

	public long growStruct(int structId,
						   StructType fieldType,
						   int fieldDim,
						   byte[] name) {
		assert(null!=fieldType);
		//grow all the arrays with new value
		assert((IS_STRUCT_BIT&structId)!=0) : "must be valid struct";
		int idx = STRUCT_MASK & structId;
		int newFieldIdx = fieldNames[idx].length;
		
		//add text lookup
		assert(isNotAlreadyDefined(this.fields[idx], name)) : "field of this name already defined.";

		
		//only 1 name is returned, the first is considered canonical
		this.fieldNames[idx] = ArrayGrow.appendToArray(this.fieldNames[idx], name); 
		this.fieldTypes[idx] = ArrayGrow.appendToArray(this.fieldTypes[idx], fieldType, StructType.class);
		this.fieldDims[idx] = ArrayGrow.appendToArray(this.fieldDims[idx], fieldDim);
		this.fieldLocals[idx] = ArrayGrow.appendToArray(this.fieldLocals[idx], null, Object.class);
		this.fieldValidators[idx] = ArrayGrow.appendToArray(this.fieldValidators[idx], null, Object.class);
		this.fieldRequired[idx] = ArrayGrow.appendToArray(this.fieldRequired[idx], false);		
						
		long fieldId = ((long)(IS_STRUCT_BIT|(STRUCT_MASK & structId)))<<STRUCT_OFFSET | newFieldIdx;
		this.fields[idx].setValue(name, fieldId);
		return fieldId;
	
	}
		
	public long modifyStruct(int structId,
						   byte[] fieldName, int fieldPos, int fieldLen,
						   StructType fieldType,
						   int fieldDim) {
		assert((IS_STRUCT_BIT&structId)!=0) : "must be valid struct";
		int idx = STRUCT_MASK & structId;
		long fieldIdx = fieldIdLookup(this.fields[idx], fieldName, fieldPos, fieldLen, Integer.MAX_VALUE);
		
		
		if (-1 == fieldIdx) {
			//only creating copy here because we know it will be held for the run.
			fieldIdx = growStruct(structId, fieldType, fieldDim, Arrays.copyOfRange(fieldName,fieldPos,fieldLen));
		} else {
			//////////
			//modify an existing field, we have new data to apply
			//////////
			//use the newest defined type
			this.fieldTypes[idx][ FIELD_MASK&(int)fieldIdx] = fieldType;		
			//keep largest dim value
			this.fieldDims[idx][ FIELD_MASK&(int)fieldIdx] = Math.max(this.fieldDims[idx][FIELD_MASK&(int)fieldIdx], fieldDim);
		}
		//System.out.println("modify struct "+fieldIdx+"   "+Appendables.appendUTF8(new StringBuilder(),   fieldName, fieldPos, fieldLen, Integer.MAX_VALUE));
		
		return fieldIdx;
		
	}
	
	public Object structAssociatedObject(int structId) {
		return structLocals[STRUCT_MASK&structId];
	}
			 		
	
	public long[] newFieldIds(int structId) {
		assert((IS_STRUCT_BIT&structId)!=0) : "must be valid struct";
		int c = fieldNames.length;
		assert(c<=FIELD_MASK) : "too many fields";
		
		long[] fieldIds = new long[c];
		long base = ((long)(STRUCT_MASK & structId))<<STRUCT_OFFSET;
		while (--c>=0) {
			fieldIds[c] =  base | (long)c;
		}
		return fieldIds;
	}
	
	
	public void setValidator(final long id, JSONRequired isRequired, Object validator) {
		assert(	(null == validator
				|| validator instanceof LongValidator
				|| validator instanceof ByteSequenceValidator
				|| validator instanceof DecimalValidator )) : "unsupported validator";
		
		int structIdx = extractStructId(id);
		int fieldIdx = extractFieldPosition(id);

		assert(structIdx < fieldLocals.length);
		assert(fieldIdx < fieldLocals[structIdx].length);		
		
		setValidator(isRequired, validator, structIdx, fieldIdx);
	}


	public void setValidator(JSONRequired isRequired, Object validator, int structIdx, int fieldIdx) {
		fieldValidators[structIdx][fieldIdx] = validator;
		fieldRequired[structIdx][fieldIdx] = (JSONRequired.REQUIRED == isRequired);
	}
	
	public boolean setAssociatedObject(final long fieldId, Object localObject) {
		assert(null!=localObject) : "must not be null, not supported";
		
		int structIdx = extractStructId(fieldId);
		int fieldIdx = extractFieldPosition(fieldId);

		assert(structIdx < fieldLocals.length);
		assert(fieldIdx < fieldLocals[structIdx].length);		
		
		return setAssoc(localObject, structIdx, fieldIdx);
	}
	
	private boolean setAssoc(Object localObject, int structIdx, int fieldIdx) {
		assert(this.fieldLocals[structIdx][fieldIdx]==null) : "associated object may only be set once. Already set to: "+this.fieldLocals[structIdx][fieldIdx];
		this.fieldLocals[structIdx][fieldIdx] = localObject;
		
		return addAssocHashToTable(localObject, structIdx, fieldIdx);
	}


	private boolean addAssocHashToTable(Object localObject, int structIdx, int fieldIdx) {
		if (null==this.fieldAttachedIndex[structIdx]) {			
			this.fieldAttachedIndex[structIdx] = new IntHashTable(
						IntHashTable.computeBits(this.fieldLocals[structIdx].length*2)
					);
		}
		
		int hashCode = localObject.hashCode();
		assert(0!=hashCode) : "can not insert null";
		assert(!IntHashTable.hasItem(this.fieldAttachedIndex[structIdx], hashCode)) : "These objects are too similar or was attached twice, Hash must be unique. Choose different objects";
		if (IntHashTable.hasItem(this.fieldAttachedIndex[structIdx], hashCode)) {
			logger.warn("Unable to add object {} as an association, Another object with an identical Hash is already held. Try a different object.", localObject);		
			return false;
		} else {
			if (!IntHashTable.setItem(this.fieldAttachedIndex[structIdx], hashCode, fieldIdx)) {
				//we are out of space
				this.fieldAttachedIndex[structIdx] = IntHashTable.doubleSize(this.fieldAttachedIndex[structIdx]);
				if (!IntHashTable.setItem(this.fieldAttachedIndex[structIdx], hashCode, fieldIdx)) {
					throw new RuntimeException("internal error");
				}
				
			} else {
				//logger.info("{} set object {} to index {}",structIdx, localObject, fieldIdx);
			}
		
			assert(fieldIdx == IntHashTable.getItem(this.fieldAttachedIndex[structIdx], hashCode));

			return true;
		}
	}

	public static int extractStructId(final long id) {
		return STRUCT_MASK&(int)(id>>>STRUCT_OFFSET);
	}
	
	public static int extractFieldPosition(long fieldId) {
		return FIELD_MASK&(int)fieldId;
	}
	
	public <T extends Object> T getAssociatedObject(long fieldId) {
		assert(fieldId>=0) : "bad fieldId";
		return (T) this.fieldLocals[extractStructId(fieldId)][extractFieldPosition(fieldId)];
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
		return fieldDims[extractStructId(id)][extractFieldPosition(id)];
	}
	
	public StructType fieldType(long id) {
		return fieldTypes[extractStructId(id)][extractFieldPosition(id)];
	}
	
	public byte[] fieldName(long id) {
		return fieldNames[extractStructId(id)][extractFieldPosition(id)];
	}
	
	public Object fieldValidator(long id) {
		return fieldValidators[extractStructId(id)][extractFieldPosition(id)];
	}
	
	public Object fieldValidator(int structId, int fieldId) {
		return fieldValidators[STRUCT_MASK&structId][extractFieldPosition(FIELD_MASK&fieldId)];
	}
	
	public long fieldLookup(CharSequence sequence, int structId) {
		assert ((IS_STRUCT_BIT&structId) !=0 ) : "Struct Id must be passed in";
		assert (structId>=0) : "Bad Struct ID "+structId;
		return TrieParserReader.query(TrieParserReaderLocal.get(), fields[STRUCT_MASK&structId], sequence);
	}
	
	public long fieldLookup(byte[] source, int pos, int len, int mask, int structId) {
		assert ((IS_STRUCT_BIT&structId) !=0 ) : "Struct Id must be passed in";
		assert (structId>=0) : "Bad Struct ID "+structId;
		TrieParserReader reader = TrieParserReaderLocal.get();
		return TrieParserReader.query(reader, fields[STRUCT_MASK&structId], source, pos, len, mask);
	}
	
	public <T> long fieldLookupByIdentity(T attachedObject, int structId) {
		
		assert ((IS_STRUCT_BIT&structId) !=0 && (structId>0) ) : "Struct Id must be passed in, got "+structId;

		int idx = lookupIndexOffset(this, attachedObject, structId, attachedObject.hashCode());
		return buildFieldId(attachedObject, structId, idx);
	}
	
	public int structLookupByIdentity(Object assoc) {
		final int hash = assoc.hashCode();
		final int idx = IntHashTable.getItem(structTable, hash);
		if (0==idx) {
			if (!IntHashTable.hasItem(structTable, hash)) {
				throw new UnsupportedOperationException("Object not found: "+assoc);			
			}
		}
		return idx;
	}

	private <T> long buildFieldId(T attachedObject, int structId, int idx) {
		assert(this.fieldLocals[STRUCT_MASK&structId][FIELD_MASK&idx] == attachedObject) : "looking for "+attachedObject+" but found "+this.fieldLocals[STRUCT_MASK&structId][FIELD_MASK&idx];

		return ((long)structId)<<STRUCT_OFFSET | (long)idx;
	}

	public static <T> int lookupIndexOffset(StructRegistry that, T attachedObject, int structId, int hash) {
				
		assert ((IS_STRUCT_BIT&structId) !=0 && (structId>0) ) : "Struct Id must be passed in, got "+structId;
		
		int idx = IntHashTable.getItem(that.fieldAttachedIndex[STRUCT_MASK&structId], hash);
		if (0==idx) {
			if (!IntHashTable.hasItem(that.fieldAttachedIndex[STRUCT_MASK&structId], hash)) {
				throw new UnsupportedOperationException("Object not found: "+attachedObject+" in structure "+structId+" obj hash "+hash);			
			}
		}
		return idx;
	}
	
	
	public void visitAssociatedObject(int structId, AssocVisitor visitor) {
		int structIdx = StructRegistry.STRUCT_MASK&structId;
				
		Object[]   objects = fieldLocals[structIdx];
		int[]         dims = fieldDims[structIdx];
		StructType[] types = fieldTypes[structIdx];
						
		for(int i=0; i<objects.length; i++) {
			
			if (null != objects[i]) {					
				visitor.visit(objects[i],dims[i],types[i]);
			}
		
		}
	}
	
	
	private void grow(int records) {
		if (records>fields.length) {
			int newSize = records*2;
			
			structLocals       = grow(newSize, structLocals);
			
			fields             = grow(newSize, fields);
			fieldNames         = grow(newSize, fieldNames);
			fieldTypes         = grow(newSize, fieldTypes);
			fieldDims          = grow(newSize, fieldDims);
			fieldLocals        = grow(newSize, fieldLocals);
			fieldAttachedIndex = grow(newSize, fieldAttachedIndex);	
			fieldValidators    = grow(newSize, fieldValidators);
			fieldRequired      = grow(newSize, fieldRequired);
			
		}
	}

	private static boolean[][] grow(int newSize, boolean[][] source) {
		boolean[][] result = new boolean[newSize][];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private static IntHashTable[] grow(int newSize, IntHashTable[] source) {
		IntHashTable[] result = new IntHashTable[newSize];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}

	private static StructType[][] grow(int newSize, StructType[][] source) {
		StructType[][] result = new StructType[newSize][];
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

	private static Object[] grow(int newSize, Object[] source) {
		Object[] result = new Object[newSize];
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
	
	public int maxDim() {
		return maxDims;
	}

	public <T> boolean visit(DataInputBlobReader<?> reader, 
			              Class<T> attachedInstanceOf, 
			              StructFieldVisitor<T> visitor) {
				
		boolean result = false;
		int structId = DataInputBlobReader.getStructType(reader);
		long fieldIdBase = ((long)(IS_STRUCT_BIT|(STRUCT_MASK & structId)))<<STRUCT_OFFSET;
		
		if (structId>0) {
			
			Object[] locals = this.fieldLocals[StructRegistry.STRUCT_MASK & structId];
			byte[][] names = this.fieldNames[StructRegistry.STRUCT_MASK & structId];
			
			for(int i = 0; i<locals.length; i++) {
				if (attachedInstanceOf.isInstance(locals[i])) {
					int readFromLastInt = DataInputBlobReader.readFromLastInt(reader, i);
					//if no value then do not visit
					if (readFromLastInt>=0 && readFromLastInt<reader.length()) {
						result = true;
						DataInputBlobReader.position(reader, readFromLastInt);	
						
						//logger.info("visit struct id {} {}",structId, Integer.toHexString(structId));
						//logger.info("bbb reading {} from position {} pos {} from pipe {}",locals[i], readFromLastInt, i, reader.getBackingPipe(reader).id );
												
						visitor.read((T) locals[i], reader, fieldIdBase | i);
					}
				}
			}
		}
		return result;
	}
	
	public <T> boolean visitNotClass(DataInputBlobReader<?> reader,
						             Class<T> attachedNotInstanceOf, 
						             StructFieldVisitor<T> visitor) {
			
		
		
		boolean result = false;
		int structId = DataInputBlobReader.getStructType(reader);
		long fieldIdBase = ((long)(IS_STRUCT_BIT|(STRUCT_MASK & structId)))<<STRUCT_OFFSET;
		if (structId>0) {
		
			Object[] locals = this.fieldLocals[StructRegistry.STRUCT_MASK & structId];

			for(int i = 0; i<locals.length; i++) {
				if (!attachedNotInstanceOf.isInstance(locals[i])) {
					int readFromLastInt = DataInputBlobReader.readFromLastInt(reader, i);
					//if no value then do not visit
					if (readFromLastInt>=0 && readFromLastInt<reader.length()) {
						result = true;
						DataInputBlobReader.position(reader, readFromLastInt);	
						
						//logger.info("visit struct id {} {}",structId, Integer.toHexString(structId));
						//logger.info("bbb reading {} from position {} pos {} from pipe {}",locals[i], readFromLastInt, i, reader.getBackingPipe(reader).id );
												
						visitor.read((T) locals[i], reader, fieldIdBase | i);
					}
				}
			}
		}
		return result;
	}
	
	public <T> boolean identityVisit(DataInputBlobReader<?> reader, T attachedObject, StructFieldVisitor<T> visitor) {

		int structId = DataInputBlobReader.getStructType(reader);
		long fieldIdBase = ((long)(IS_STRUCT_BIT|(STRUCT_MASK & structId)))<<STRUCT_OFFSET;
		
		int idx =  StructRegistry.FIELD_MASK & (int)fieldLookupByIdentity(attachedObject, structId);
		if (idx>=0) {
			int readFromLastInt = DataInputBlobReader.readFromLastInt(reader, idx);
			if (readFromLastInt>=0) {
				DataInputBlobReader.position(reader, readFromLastInt);
				visitor.read((T)(fieldLocals[STRUCT_MASK&structId][idx]), reader, fieldIdBase | idx);
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	public int totalSizeOfIndexes(int structId) {
		assert ((IS_STRUCT_BIT&structId) !=0 ) : "Struct Id must be passed in";
		assert (structId>=0) : "Bad Struct ID "+structId;
		return fieldTypes[STRUCT_MASK & structId].length;
	}


	public int fieldCount(int structId) {
		assert ((IS_STRUCT_BIT&structId) !=0 ) : "Struct Id must be passed in";
		assert (structId>=0) : "Bad Struct ID "+structId;
		return fieldNames[STRUCT_MASK & structId].length;
	}
	
	public long buildFieldId(int structId, int fieldPosition) {
		assert ((IS_STRUCT_BIT&structId) !=0 ) : "Struct Id must be passed in";
		assert (structId>=0) : "Bad Struct ID "+structId;
		if (fieldPosition>=fieldNames[STRUCT_MASK & structId].length) {
			throw new ArrayIndexOutOfBoundsException(fieldPosition);
		}
		return (((long)(IS_STRUCT_BIT|(STRUCT_MASK & structId)))<<STRUCT_OFFSET) | fieldPosition;
	}

	
}